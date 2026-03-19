// ---------------------------------------------------------------------------
// PEP (Politically Exposed Persons) ingestion from Wikidata SPARQL endpoint.
//
// Wikidata is CC0 licensed, free for commercial use, updated continuously.
//
// Strategy: Query politicians per country to keep result sets manageable.
// Wikidata's ontology is inconsistent for class hierarchy traversal, so
// we use occupation=politician (P106=Q82955) combined with position held
// (P39) and require an English Wikipedia article for notability filtering.
//
// Supplementary queries for judges and central bank governors use the
// class hierarchy approach (P31/P279*) which works for those categories.
// ---------------------------------------------------------------------------

import type { SanctionEntity } from "./parse.js";

const SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
const USER_AGENT = "Argvs PEP Screener/1.0.0 (https://github.com/anthropics/argvs; contact: argvs@vigil.dev)";
const PAGE_SIZE = 10_000;

// Minimum end date: 20 years ago
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
  country?: { value: string };
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
// Convert SPARQL bindings to SanctionEntity[]
// ---------------------------------------------------------------------------

function bindingsToEntities(
  bindings: SparqlBinding[],
  countryName: string
): Map<string, SanctionEntity> {
  const byId = new Map<string, SanctionEntity>();

  for (const b of bindings) {
    const wikidataId = b.wikidataId?.value ?? "";
    if (!wikidataId) continue;

    const personLabel = b.personLabel?.value ?? "";
    if (!personLabel || personLabel === wikidataId) continue;

    const position = b.positionLabel?.value ?? "";
    const country = b.countryLabel?.value ?? countryName;
    const startDate = b.startDate?.value?.slice(0, 10) ?? "";
    const endDate = b.endDate?.value?.slice(0, 10) ?? "";

    const existing = byId.get(wikidataId);
    if (existing) {
      if (position) {
        const posSet = new Set(existing.programs);
        posSet.add(position);
        existing.programs = [...posSet];
        existing.remarks = `Position(s): ${existing.programs.join("; ")}`;
      }
    } else {
      byId.set(wikidataId, {
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
      });
    }
  }

  return byId;
}

function mergeInto(
  target: Map<string, SanctionEntity>,
  source: Map<string, SanctionEntity>
): void {
  for (const [id, entity] of source) {
    const existing = target.get(id);
    if (existing) {
      const posSet = new Set([...existing.programs, ...entity.programs]);
      existing.programs = [...posSet];
      const countrySet = new Set([...existing.countries, ...entity.countries]);
      existing.countries = [...countrySet];
      existing.remarks = `Position(s): ${existing.programs.join("; ")}`;
    } else {
      target.set(id, entity);
    }
  }
}

// ---------------------------------------------------------------------------
// G20 countries (reduced scope for reliability)
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
): Promise<Map<string, SanctionEntity>> {
  const allEntities = new Map<string, SanctionEntity>();
  let offset = 0;

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
      break; // Keep what we have
    }

    const entities = bindingsToEntities(bindings, countryLabel);
    mergeInto(allEntities, entities);

    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return allEntities;
}

// ---------------------------------------------------------------------------
// Role-specific queries (judges, central bank governors)
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
  cutoff: string
): Promise<Map<string, SanctionEntity>> {
  const allEntities = new Map<string, SanctionEntity>();
  let offset = 0;

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

    const entities = bindingsToEntities(bindings, "");
    mergeInto(allEntities, entities);

    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return allEntities;
}

// ---------------------------------------------------------------------------
// Main PEP ingestion
// ---------------------------------------------------------------------------

export async function ingestPep(): Promise<SanctionEntity[]> {
  console.log("\n=== PEP (Politically Exposed Persons) Ingestion ===\n");
  console.log("Source: Wikidata SPARQL endpoint (CC0 license)");

  const cutoff = cutoffDate();
  console.log(`Cutoff date: ${cutoff} (20 years)\n`);

  const allEntities = new Map<string, SanctionEntity>();

  // Phase 1: Fetch politicians for G20 countries
  console.log("Phase 1: Politicians by country (G20 scope)...");
  const countries = G20_COUNTRIES;
  console.log(`  ${countries.length} G20 countries`);

  let countryIdx = 0;
  for (const country of countries) {
    countryIdx++;
    try {
      const entities = await fetchPoliticiansByCountry(
        country.id,
        country.label,
        cutoff
      );
      if (entities.size > 0) {
        mergeInto(allEntities, entities);
        console.log(
          `  [${countryIdx}/${countries.length}] ${country.label}: ${entities.size} politicians`
        );
      }
    } catch {
      // Skip failed countries silently
    }
    await delay(1500); // Rate limit between countries
  }

  console.log(`  Politicians total: ${allEntities.size}\n`);

  // Phase 2: Role-specific queries (judges, central bank)
  console.log("Phase 2: Role-specific queries...");
  for (const rq of ROLE_QUERIES) {
    console.log(`  Fetching ${rq.label}...`);
    try {
      const entities = await fetchRoleQuery(rq, cutoff);
      mergeInto(allEntities, entities);
      console.log(`  ${rq.label}: ${entities.size} persons`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  Failed: ${rq.label}: ${msg}`);
    }
    await delay(2000);
  }

  const result = [...allEntities.values()];
  console.log(`\nPEP ingestion complete: ${result.length} unique persons`);
  return result;
}
