import { initDatabase, getInsertBatch, getDb } from "./db.js";
import { screenEntity } from "./match.js";

// Quick test: ingest PEP data for just 3 countries + role queries, then screen
const SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
const USER_AGENT = "Argvs PEP Screener/1.0.0 (contact: argvs@vigil.dev)";
const PAGE_SIZE = 10_000;

function cutoffDate(): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() - 20);
  return d.toISOString().slice(0, 10);
}

async function sparql(q: string): Promise<any[]> {
  const url = new URL(SPARQL_ENDPOINT);
  url.searchParams.set("query", q);
  url.searchParams.set("format", "json");
  const resp = await fetch(url.toString(), {
    headers: { Accept: "application/sparql-results+json", "User-Agent": USER_AGENT },
    signal: AbortSignal.timeout(120_000),
  });
  if (!resp.ok) throw new Error(`SPARQL ${resp.status}`);
  return JSON.parse(await resp.text()).results.bindings;
}

interface SanctionEntity {
  id: string; source: string; name: string; aliases: string[];
  type: "individual" | "entity" | "vessel" | "aircraft" | "unknown";
  programs: string[]; countries: string[]; identifiers: string[];
  date_listed: string; remarks: string;
}

async function fetchCountry(qid: string, label: string, cutoff: string): Promise<SanctionEntity[]> {
  const entities = new Map<string, SanctionEntity>();
  let offset = 0;
  while (true) {
    let bindings: any[];
    try {
      bindings = await sparql(`
SELECT DISTINCT ?wikidataId ?personLabel ?positionLabel ?startDate ?endDate WHERE {
  ?person wdt:P106 wd:Q82955 .
  ?person wdt:P27 wd:${qid} .
  ?person p:P39 ?stmt .
  ?stmt ps:P39 ?position .
  ?article schema:about ?person .
  ?article schema:isPartOf <https://en.wikipedia.org/> .
  ?person rdfs:label ?personLabel . FILTER(LANG(?personLabel) = "en")
  ?position rdfs:label ?positionLabel . FILTER(LANG(?positionLabel) = "en")
  OPTIONAL { ?stmt pq:P580 ?startDate . }
  OPTIONAL { ?stmt pq:P582 ?endDate . }
  FILTER(!BOUND(?endDate) || ?endDate >= "${cutoff}"^^xsd:dateTime)
  BIND(REPLACE(STR(?person), "http://www.wikidata.org/entity/", "") AS ?wikidataId)
}
LIMIT ${PAGE_SIZE} OFFSET ${offset}`);
    } catch { break; }

    for (const b of bindings) {
      const wid = b.wikidataId?.value ?? "";
      const name = b.personLabel?.value ?? "";
      if (!wid || !name || name === wid) continue;
      const pos = b.positionLabel?.value ?? "";
      const existing = entities.get(wid);
      if (existing) {
        if (pos) { const s = new Set(existing.programs); s.add(pos); existing.programs = [...s]; existing.remarks = `Position(s): ${existing.programs.join("; ")}`; }
      } else {
        entities.set(wid, {
          id: `pep:${wid}`, source: "PEP", name, aliases: [], type: "individual",
          programs: pos ? [pos] : [], countries: [label], identifiers: [wid],
          date_listed: b.startDate?.value?.slice(0, 10) ?? "",
          remarks: pos ? `Position(s): ${pos}` : "",
        });
      }
    }
    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await new Promise(r => setTimeout(r, 3000));
  }
  return [...entities.values()];
}

async function main() {
  initDatabase();

  const cutoff = cutoffDate();
  console.log(`Cutoff: ${cutoff}\n`);

  // Fetch politicians from France, Italy, US
  const testCountries = [
    { qid: "Q142", label: "France" },
    { qid: "Q38", label: "Italy" },
    { qid: "Q30", label: "United States" },
  ];

  let totalPep = 0;
  for (const c of testCountries) {
    console.log(`Fetching ${c.label}...`);
    const start = Date.now();
    const entities = await fetchCountry(c.qid, c.label, cutoff);
    console.log(`  ${entities.length} politicians in ${Date.now() - start}ms`);
    if (entities.length > 0) {
      const batch = getInsertBatch();
      for (const e of entities) batch.add(e);
      batch.flush();
      totalPep += entities.length;
    }
    await new Promise(r => setTimeout(r, 3000));
  }

  // Also fetch judges and central bank governors
  console.log("\nFetching judges...");
  const judges = await sparql(`
SELECT DISTINCT ?wikidataId ?personLabel ?countryLabel ?positionLabel ?startDate ?endDate WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 ?position .
  { ?position wdt:P31/wdt:P279* wd:Q30461 . }
  UNION
  { ?position wdt:P31/wdt:P279* wd:Q3400985 . }
  ?person wdt:P27 ?country .
  ?person rdfs:label ?personLabel . FILTER(LANG(?personLabel) = "en")
  ?position rdfs:label ?positionLabel . FILTER(LANG(?positionLabel) = "en")
  ?country rdfs:label ?countryLabel . FILTER(LANG(?countryLabel) = "en")
  OPTIONAL { ?stmt pq:P580 ?startDate . }
  OPTIONAL { ?stmt pq:P582 ?endDate . }
  FILTER(!BOUND(?endDate) || ?endDate >= "${cutoff}"^^xsd:dateTime)
  BIND(REPLACE(STR(?person), "http://www.wikidata.org/entity/", "") AS ?wikidataId)
}
LIMIT 10000`);

  const judgeEntities: SanctionEntity[] = [];
  const seen = new Set<string>();
  for (const b of judges) {
    const wid = b.wikidataId?.value ?? "";
    const name = b.personLabel?.value ?? "";
    if (!wid || !name || name === wid || seen.has(wid)) continue;
    seen.add(wid);
    judgeEntities.push({
      id: `pep:${wid}`, source: "PEP", name, aliases: [], type: "individual",
      programs: [b.positionLabel?.value ?? "judge"], countries: [b.countryLabel?.value ?? ""],
      identifiers: [wid], date_listed: b.startDate?.value?.slice(0, 10) ?? "",
      remarks: `Position(s): ${b.positionLabel?.value ?? "judge"}`,
    });
  }
  console.log(`  ${judgeEntities.length} judges`);
  if (judgeEntities.length > 0) { const b1 = getInsertBatch(); for (const e of judgeEntities) b1.add(e); b1.flush(); totalPep += judgeEntities.length; }

  await new Promise(r => setTimeout(r, 3000));

  console.log("\nFetching central bank governors...");
  const cbg = await sparql(`
SELECT DISTINCT ?wikidataId ?personLabel ?countryLabel ?positionLabel ?startDate ?endDate WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 ?position .
  ?position wdt:P31/wdt:P279* wd:Q889821 .
  ?person wdt:P27 ?country .
  ?person rdfs:label ?personLabel . FILTER(LANG(?personLabel) = "en")
  ?position rdfs:label ?positionLabel . FILTER(LANG(?positionLabel) = "en")
  ?country rdfs:label ?countryLabel . FILTER(LANG(?countryLabel) = "en")
  OPTIONAL { ?stmt pq:P580 ?startDate . }
  OPTIONAL { ?stmt pq:P582 ?endDate . }
  FILTER(!BOUND(?endDate) || ?endDate >= "${cutoff}"^^xsd:dateTime)
  BIND(REPLACE(STR(?person), "http://www.wikidata.org/entity/", "") AS ?wikidataId)
}
LIMIT 10000`);

  const cbgEntities: SanctionEntity[] = [];
  for (const b of cbg) {
    const wid = b.wikidataId?.value ?? "";
    const name = b.personLabel?.value ?? "";
    if (!wid || !name || name === wid || seen.has(wid)) continue;
    seen.add(wid);
    cbgEntities.push({
      id: `pep:${wid}`, source: "PEP", name, aliases: [], type: "individual",
      programs: [b.positionLabel?.value ?? "central bank governor"], countries: [b.countryLabel?.value ?? ""],
      identifiers: [wid], date_listed: b.startDate?.value?.slice(0, 10) ?? "",
      remarks: `Position(s): ${b.positionLabel?.value ?? "central bank governor"}`,
    });
  }
  console.log(`  ${cbgEntities.length} central bank governors`);
  if (cbgEntities.length > 0) { const b2 = getInsertBatch(); for (const e of cbgEntities) b2.add(e); b2.flush(); totalPep += cbgEntities.length; }

  const db = getDb();
  const pepCount = (db.prepare("SELECT COUNT(*) as count FROM entities WHERE source = 'PEP'").get() as any).count;
  console.log(`\nTotal PEP entities in DB: ${pepCount}`);

  // Test screening
  const testNames = ["Emmanuel Macron", "Mario Draghi", "Janet Yellen"];
  for (const name of testNames) {
    console.log(`\n--- Screening: ${name} ---`);
    const result = screenEntity({ name });
    console.log(`  Risk level: ${result.risk_level}`);
    console.log(`  PEP status: ${result.pep_status}`);
    console.log(`  Matches: ${result.matches.length}`);
    for (const m of result.matches) {
      console.log(`  [${m.confidence}%] ${m.name} (source: ${m.source})`);
      console.log(`    Programs: ${m.programs.join(", ")}`);
      console.log(`    Countries: ${m.countries.join(", ")}`);
      if (m.position) console.log(`    Position: ${m.position}`);
    }
  }
}

main().catch(console.error);
