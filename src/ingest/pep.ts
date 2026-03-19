// ---------------------------------------------------------------------------
// PEP (Politically Exposed Persons) ingestion from Wikidata SPARQL endpoint.
//
// Memory-safe: processes one country at a time, inserts into SQLite
// immediately, then frees the array. Checks heap before each country.
// ---------------------------------------------------------------------------

import type { SanctionEntity } from "./parse.js";
import { tryGc, logHeap, getHeapMB } from "./parse.js";
import { getInsertBatch } from "../db.js";

const SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
const USER_AGENT = "Argvs PEP Screener/1.0.0 (https://github.com/anthropics/argvs; contact: argvs@vigil.dev)";
const PAGE_SIZE = 10_000;
const HEAP_LIMIT_MB = 300;
const HEAP_SKIP_MB = 250;

function cutoffDate(): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() - 20);
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// SPARQL execution
// ---------------------------------------------------------------------------

interface SparqlBinding {
  wikidataId?: { value: string };
  personLabel?: { value: string };
  countryLabel?: { value: string };
  positionLabel?: { value: string };
  startDate?: { value: string };
  endDate?: { value: string };
}

async function runSparqlQuery(sparql: string): Promise<SparqlBinding[]> {
  const url = new URL(SPARQL_ENDPOINT);
  url.searchParams.set("query", sparql);
  url.searchParams.set("format", "json");

  const resp = await fetch(url.toString(), {
    headers: {
      Accept: "application/sparql-results+json",
      "User-Agent": USER_AGENT,
    },
    signal: AbortSignal.timeout(5_000),
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`SPARQL ${resp.status}: ${text.slice(0, 200)}`);
  }

  const text = await resp.text();
  let json: { results: { bindings: SparqlBinding[] } };
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`JSON parse failed (${text.length} bytes)`);
  }
  return json.results.bindings;
}

async function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Convert SPARQL bindings to entities and insert immediately
// ---------------------------------------------------------------------------

function processBindings(
  bindings: SparqlBinding[],
  countryName: string,
  existing: Map<string, SanctionEntity>
): SanctionEntity[] {
  const newEntities: SanctionEntity[] = [];

  for (const b of bindings) {
    const wikidataId = b.wikidataId?.value ?? "";
    if (!wikidataId) continue;

    const personLabel = b.personLabel?.value ?? "";
    if (!personLabel || personLabel === wikidataId) continue;

    const position = b.positionLabel?.value ?? "";
    const country = b.countryLabel?.value ?? countryName;
    const startDate = b.startDate?.value?.slice(0, 10) ?? "";
    const endDate = b.endDate?.value?.slice(0, 10) ?? "";

    const ex = existing.get(wikidataId);
    if (ex) {
      if (position) {
        const posSet = new Set(ex.programs);
        posSet.add(position);
        ex.programs = [...posSet];
        ex.remarks = `Position(s): ${ex.programs.join("; ")}`;
      }
    } else {
      const entity: SanctionEntity = {
        id: `pep:${wikidataId}`,
        source: "PEP",
        name: personLabel,
        aliases: [],
        type: "individual",
        programs: position ? [position] : [],
        countries: country ? [country] : [],
        identifiers: [wikidataId],
        date_listed: startDate,
        remarks: position
          ? `Position(s): ${position}${endDate ? ` (ended ${endDate})` : ""}`
          : "",
      };
      existing.set(wikidataId, entity);
      newEntities.push(entity);
    }
  }

  return newEntities;
}

// ---------------------------------------------------------------------------
// G20 countries
// ---------------------------------------------------------------------------

const G20_COUNTRIES: Array<{ id: string; label: string }> = [
  { id: "Q30", label: "United States" },
  { id: "Q145", label: "United Kingdom" },
  { id: "Q142", label: "France" },
  { id: "Q183", label: "Germany" },
  { id: "Q38", label: "Italy" },
  { id: "Q17", label: "Japan" },
  { id: "Q16", label: "Canada" },
  { id: "Q408", label: "Australia" },
  { id: "Q155", label: "Brazil" },
  { id: "Q414", label: "Argentina" },
  { id: "Q96", label: "Mexico" },
  { id: "Q884", label: "South Korea" },
  { id: "Q668", label: "India" },
  { id: "Q148", label: "China" },
  { id: "Q159", label: "Russia" },
  { id: "Q258", label: "South Africa" },
  { id: "Q851", label: "Saudi Arabia" },
  { id: "Q43", label: "Turkey" },
  { id: "Q252", label: "Indonesia" },
  { id: "Q458", label: "European Union" },
];

// ---------------------------------------------------------------------------
// Per-country politician query with pagination
// ---------------------------------------------------------------------------

async function fetchPoliticiansByCountry(
  countryQid: string,
  countryLabel: string,
  cutoff: string
): Promise<SanctionEntity[]> {
  const seen = new Map<string, SanctionEntity>();
  let offset = 0;
  const allNew: SanctionEntity[] = [];

  while (true) {
    const sparql = `
SELECT DISTINCT ?wikidataId ?personLabel ?positionLabel ?startDate ?endDate WHERE {
  ?person wdt:P106 wd:Q82955 .
  ?person wdt:P27 wd:${countryQid} .
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
LIMIT ${PAGE_SIZE} OFFSET ${offset}`;

    let bindings: SparqlBinding[];
    try {
      bindings = await runSparqlQuery(sparql);
    } catch {
      break;
    }

    const newOnes = processBindings(bindings, countryLabel, seen);
    allNew.push(...newOnes);

    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return allNew;
}

// ---------------------------------------------------------------------------
// Role-specific queries
// ---------------------------------------------------------------------------

interface RoleQuery {
  label: string;
  sparql: (offset: number, cutoff: string) => string;
}

const ROLE_QUERIES: RoleQuery[] = [
  {
    label: "Supreme/constitutional court judges",
    sparql: (offset, cutoff) => `
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
LIMIT ${PAGE_SIZE} OFFSET ${offset}`,
  },
  {
    label: "Central bank governors",
    sparql: (offset, cutoff) => `
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
LIMIT ${PAGE_SIZE} OFFSET ${offset}`,
  },
];

async function fetchRoleQuery(
  rq: RoleQuery,
  cutoff: string,
  seen: Map<string, SanctionEntity>
): Promise<SanctionEntity[]> {
  let offset = 0;
  const allNew: SanctionEntity[] = [];

  while (true) {
    const sparql = rq.sparql(offset, cutoff);
    let bindings: SparqlBinding[];
    try {
      bindings = await runSparqlQuery(sparql);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  Warning: ${rq.label} failed at offset ${offset}: ${msg}`);
      break;
    }

    const newOnes = processBindings(bindings, "", seen);
    allNew.push(...newOnes);

    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return allNew;
}

// ---------------------------------------------------------------------------
// Main PEP ingestion — inserts into SQLite per country, memory-safe
// ---------------------------------------------------------------------------

export async function ingestPep(): Promise<number> {
  console.log("\n=== PEP (Politically Exposed Persons) Ingestion ===\n");
  console.log("Source: Wikidata SPARQL endpoint (CC0 license)");

  // Check memory before starting
  const startHeap = getHeapMB();
  if (startHeap > HEAP_SKIP_MB) {
    console.warn(`[pep] Heap at ${startHeap.toFixed(0)}MB > ${HEAP_SKIP_MB}MB — skipping PEP ingestion entirely`);
    return 0;
  }

  const cutoff = cutoffDate();
  console.log(`Cutoff date: ${cutoff} (20 years)\n`);

  const batch = getInsertBatch();
  // Track seen IDs for dedup across countries
  const seenIds = new Map<string, SanctionEntity>();

  // Phase 1: Politicians by country
  console.log("Phase 1: Politicians by country (G20 scope)...");
  let countryIdx = 0;

  for (const country of G20_COUNTRIES) {
    countryIdx++;

    // Check heap before each country
    const heapMB = getHeapMB();
    if (heapMB > HEAP_LIMIT_MB) {
      console.warn(`[pep] Heap at ${heapMB.toFixed(0)}MB > ${HEAP_LIMIT_MB}MB — skipping remaining PEP countries`);
      break;
    }

    try {
      const entities = await fetchPoliticiansByCountry(
        country.id,
        country.label,
        cutoff
      );

      // Insert into SQLite immediately
      for (const e of entities) {
        batch.add(e);
      }

      if (entities.length > 0) {
        console.log(
          `  [${countryIdx}/${G20_COUNTRIES.length}] ${country.label}: ${entities.length} politicians`
        );
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  [${countryIdx}/${G20_COUNTRIES.length}] ${country.label}: SKIPPED (${msg})`);
    }

    tryGc();
    await delay(1500);
  }

  batch.flush();
  logHeap("after PEP phase 1");

  // Phase 2: Role-specific queries
  console.log("\nPhase 2: Role-specific queries...");
  for (const rq of ROLE_QUERIES) {
    const heapMB = getHeapMB();
    if (heapMB > HEAP_LIMIT_MB) {
      console.warn(`[pep] Heap at ${heapMB.toFixed(0)}MB — skipping remaining role queries`);
      break;
    }

    console.log(`  Fetching ${rq.label}...`);
    try {
      const entities = await fetchRoleQuery(rq, cutoff, seenIds);
      for (const e of entities) {
        batch.add(e);
      }
      console.log(`  ${rq.label}: ${entities.length} persons`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  Failed: ${rq.label}: ${msg}`);
    }
    tryGc();
    await delay(2000);
  }

  const total = batch.flush();
  seenIds.clear();
  logHeap("after PEP complete");

  console.log(`\nPEP ingestion complete: ${total} persons inserted`);
  return total;
}
