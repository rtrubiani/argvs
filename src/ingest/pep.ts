// ---------------------------------------------------------------------------
// PEP (Politically Exposed Persons) ingestion from Wikidata SPARQL endpoint.
//
// Memory-safe: processes one country at a time, inserts into SQLite
// immediately via callback, uses Set<string> for dedup (not full entities).
// Checks heap at every country boundary, stops if > 250MB.
// ---------------------------------------------------------------------------

import type Database from "better-sqlite3";
import type { SanctionEntity } from "./parse.js";
import { tryGc, logHeap, getHeapMB } from "./parse.js";
import { getInsertBatch } from "../db.js";

const SPARQL_ENDPOINT = "https://query.wikidata.org/sparql";
const USER_AGENT = "Argvs PEP Screener/1.0.0 (https://github.com/anthropics/argvs; contact: argvs@vigil.dev)";
const PAGE_SIZE = 10_000;
const HEAP_STOP_MB = 250;

function cutoffDate(): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() - 20);
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// SPARQL execution — 10 second timeout per request
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
    signal: AbortSignal.timeout(10_000),
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
  const bindings = json.results.bindings;
  // Free the parsed JSON shell — we only need bindings
  (json as any).results = null;
  return bindings;
}

async function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Process a single SPARQL binding into an entity and emit via callback.
// Returns true if a new entity was emitted, false if duplicate/invalid.
// ---------------------------------------------------------------------------

function processBinding(
  b: SparqlBinding,
  countryName: string,
  seen: Set<string>,
  onEntity: (e: SanctionEntity) => void,
): boolean {
  const wikidataId = b.wikidataId?.value ?? "";
  if (!wikidataId || seen.has(wikidataId)) return false;

  const personLabel = b.personLabel?.value ?? "";
  if (!personLabel || personLabel === wikidataId) return false;

  seen.add(wikidataId);

  const position = b.positionLabel?.value ?? "";
  const country = b.countryLabel?.value ?? countryName;
  const startDate = b.startDate?.value?.slice(0, 10) ?? "";
  const endDate = b.endDate?.value?.slice(0, 10) ?? "";

  onEntity({
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
  return true;
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
// Per-country politician fetch — streams entities via callback, no arrays
// ---------------------------------------------------------------------------

async function fetchPoliticiansByCountry(
  countryQid: string,
  countryLabel: string,
  cutoff: string,
  seen: Set<string>,
  onEntity: (e: SanctionEntity) => void,
): Promise<number> {
  let offset = 0;
  let count = 0;

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
      break; // skip on failure, don't retry
    }

    const bindingCount = bindings.length;
    for (const b of bindings) {
      if (processBinding(b, countryLabel, seen, onEntity)) {
        count++;
      }
    }
    // Free bindings immediately
    bindings = null as any;

    if (bindingCount < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return count;
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
  seen: Set<string>,
  onEntity: (e: SanctionEntity) => void,
): Promise<number> {
  let offset = 0;
  let count = 0;

  while (true) {
    const sparql = rq.sparql(offset, cutoff);
    let bindings: SparqlBinding[];
    try {
      bindings = await runSparqlQuery(sparql);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  Warning: ${rq.label} failed at offset ${offset}: ${msg}`);
      break; // skip on failure, don't retry
    }

    const bindingCount = bindings.length;
    for (const b of bindings) {
      if (processBinding(b, "", seen, onEntity)) {
        count++;
      }
    }
    bindings = null as any;

    if (bindingCount < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await delay(3000);
  }

  return count;
}

// ---------------------------------------------------------------------------
// Main PEP ingestion — one country at a time, immediate insert, strict limits.
// Accepts optional targetDb for daily refresh (inserts into new db file).
// ---------------------------------------------------------------------------

export async function ingestPep(targetDb?: Database.Database): Promise<number> {
  console.log("\n=== PEP (Politically Exposed Persons) Ingestion ===\n");
  console.log("Source: Wikidata SPARQL endpoint (CC0 license)");

  const cutoff = cutoffDate();
  console.log(`Cutoff date: ${cutoff} (20 years)\n`);

  const batch = getInsertBatch(targetDb);
  // Set<string> for dedup — stores only wikidataId strings, not full entities
  const seen = new Set<string>();
  let total = 0;

  // Phase 1: Politicians by country (G20 scope)
  console.log("Phase 1: Politicians by country (G20 scope)...");

  for (let i = 0; i < G20_COUNTRIES.length; i++) {
    const country = G20_COUNTRIES[i];

    // Check heap before each country — stop if > 250MB
    const heapMB = getHeapMB();
    if (heapMB > HEAP_STOP_MB) {
      console.warn(`[pep] Heap at ${heapMB.toFixed(0)}MB > ${HEAP_STOP_MB}MB — stopping PEP ingestion, marking as ready`);
      break;
    }

    try {
      const count = await fetchPoliticiansByCountry(
        country.id,
        country.label,
        cutoff,
        seen,
        (e) => batch.add(e),
      );
      total += count;
      if (count > 0) {
        console.log(`  [${i + 1}/${G20_COUNTRIES.length}] ${country.label}: ${count} politicians`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  [${i + 1}/${G20_COUNTRIES.length}] ${country.label}: SKIPPED (${msg})`);
      // Skip, don't retry, continue with next
    }

    // Flush batch and GC after each country
    batch.flush();
    tryGc();
    await delay(1500);
  }

  logHeap("after PEP phase 1");

  // Phase 2: Role-specific queries
  console.log("\nPhase 2: Role-specific queries...");
  for (const rq of ROLE_QUERIES) {
    const heapMB = getHeapMB();
    if (heapMB > HEAP_STOP_MB) {
      console.warn(`[pep] Heap at ${heapMB.toFixed(0)}MB — skipping remaining role queries`);
      break;
    }

    console.log(`  Fetching ${rq.label}...`);
    try {
      const count = await fetchRoleQuery(rq, cutoff, seen, (e) => batch.add(e));
      total += count;
      console.log(`  ${rq.label}: ${count} persons`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  Failed: ${rq.label}: ${msg}`);
    }
    batch.flush();
    tryGc();
    await delay(2000);
  }

  batch.flush();
  seen.clear();
  logHeap("after PEP complete");

  console.log(`\nPEP ingestion complete: ${total} persons inserted`);
  return total;
}
