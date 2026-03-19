// ---------------------------------------------------------------------------
// Streaming ingestion orchestrator
//
// Architecture: Stream everything, hold nothing.
// - Download one source at a time to disk
// - Stream-parse each file, inserting into SQLite in batches of 100
// - Delete the file immediately after processing
// - Force GC between sources
// - Never have two sources in memory at once
// ---------------------------------------------------------------------------

import { sources } from "./sources.js";
import { downloadSource, deleteSourceFiles } from "./download.js";
import { streamParseSource, tryGc, logHeap, getHeapMB } from "./parse.js";
import { ingestPep } from "./pep.js";
import {
  resetDatabase,
  getInsertBatch,
  setRuntimePragmas,
  shrinkMemory,
  createRefreshDb,
  swapRefreshDb,
  abortRefresh,
  getDb,
} from "../db.js";

export interface IngestionProgress {
  state: "idle" | "downloading" | "parsing" | "loading_peps" | "ready" | "error";
  currentSource: string | null;
  sourcesLoaded: Record<string, number>;
  totalEntities: number;
  sanctionsReady: boolean;
  pepCount: number;
  errors: string[];
}

// Shared progress object — read by /api/status
export const progress: IngestionProgress = {
  state: "idle",
  currentSource: null,
  sourcesLoaded: {},
  totalEntities: 0,
  sanctionsReady: false,
  pepCount: 0,
  errors: [],
};

const HEAP_CRITICAL_MB = 350;
const HEAP_PEP_SKIP_MB = 200;

function checkHeapCritical(label: string): void {
  const mb = getHeapMB();
  if (mb > HEAP_CRITICAL_MB) {
    console.error(`[CRITICAL] Heap at ${mb.toFixed(0)}MB exceeds ${HEAP_CRITICAL_MB}MB limit — ${label}`);
  }
}

// ---------------------------------------------------------------------------
// Initial ingestion: download → stream-parse → insert → delete, one at a time
// ---------------------------------------------------------------------------

export async function runInitialIngestion(): Promise<void> {
  console.log("=== Argvs Sanctions Data Ingestion (Streaming) ===\n");
  logHeap("server startup");

  resetDatabase();
  progress.state = "downloading";
  progress.errors = [];
  progress.sourcesLoaded = {};
  progress.totalEntities = 0;

  let totalEntities = 0;

  // Process each source sequentially
  for (const source of sources) {
    progress.currentSource = source.name;
    progress.state = "downloading";
    console.log(`\n--- ${source.name} ---`);
    logHeap(`before downloading ${source.name}`);

    // 1. Download to disk
    const result = await downloadSource(source);
    if (!result.filepath) {
      progress.errors.push(`${source.name}: ${result.error}`);
      continue;
    }

    // 2. Stream-parse and insert into SQLite in batches
    progress.state = "parsing";
    try {
      const batch = getInsertBatch();
      const count = await streamParseSource(source, result.filepath, (entity) => {
        batch.add(entity);
      });
      const inserted = batch.flush();
      totalEntities += inserted;
      progress.sourcesLoaded[source.id] = inserted;
      progress.totalEntities = totalEntities;
      console.log(`  ✓ ${source.name}: ${inserted} entities inserted`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  ✗ Failed to parse ${source.name}: ${msg}`);
      progress.errors.push(`${source.name}: parse failed — ${msg}`);
    }

    // 3. Delete downloaded file immediately
    deleteSourceFiles(source);

    // 4. Force GC and log heap
    tryGc();
    logHeap(`after ${source.name}`);
    checkHeapCritical(`after ${source.name}`);
  }

  // Switch from fast ingestion pragmas to safe runtime pragmas
  setRuntimePragmas();
  shrinkMemory();
  tryGc();

  progress.currentSource = null;
  progress.sanctionsReady = true;
  progress.state = "ready";
  console.log(`\n✓ Sanctions loaded: ${totalEntities} entities. Server ready.`);
  logHeap("sanctions complete");
}

// ---------------------------------------------------------------------------
// PEP ingestion (runs after sanctions are ready, in background)
// ---------------------------------------------------------------------------

export async function runPepIngestion(): Promise<void> {
  progress.state = "loading_peps";
  progress.currentSource = "PEP (Wikidata)";

  // Check heap before starting PEPs — skip if > 200MB
  const heapMB = getHeapMB();
  logHeap("before PEP ingestion");
  if (heapMB > HEAP_PEP_SKIP_MB) {
    console.warn(`[pep] Heap at ${heapMB.toFixed(0)}MB > ${HEAP_PEP_SKIP_MB}MB — skipping PEP ingestion`);
    progress.currentSource = null;
    progress.state = "ready";
    return;
  }

  try {
    const pepCount = await ingestPep();
    progress.pepCount = pepCount;
    progress.totalEntities += pepCount;
    console.log(`\n✓ PEP ingestion complete: ${pepCount} persons`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[pep] PEP ingestion failed (non-fatal): ${msg}`);
    progress.errors.push(`PEP: ${msg}`);
  }

  shrinkMemory();
  tryGc();
  progress.currentSource = null;
  progress.state = "ready";
  logHeap("after PEP ingestion");
}

// ---------------------------------------------------------------------------
// Daily refresh: safe swap using separate database file.
// Creates sanctions_new.db, runs full ingestion into it, then swaps.
// If anything fails, the working database is untouched.
// ---------------------------------------------------------------------------

export async function runDailyRefresh(): Promise<void> {
  console.log("[cron] Starting daily sanctions + PEP data refresh...");
  logHeap("[cron] refresh start");

  const refreshDb = createRefreshDb();
  let totalEntities = 0;
  let anySourceSucceeded = false;

  for (const source of sources) {
    progress.currentSource = source.name;
    progress.state = "downloading";
    console.log(`\n[cron] --- ${source.name} ---`);
    logHeap(`[cron] before downloading ${source.name}`);

    const result = await downloadSource(source);
    if (!result.filepath) {
      progress.errors.push(`[cron] ${source.name}: ${result.error}`);
      continue;
    }

    progress.state = "parsing";
    try {
      const batch = getInsertBatch(refreshDb);
      await streamParseSource(source, result.filepath, (entity) => {
        batch.add(entity);
      });
      const inserted = batch.flush();
      totalEntities += inserted;
      anySourceSucceeded = true;
      console.log(`[cron] ✓ ${source.name}: ${inserted} entities`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[cron] ✗ Failed to parse ${source.name}: ${msg}`);
    }

    deleteSourceFiles(source);
    tryGc();
    logHeap(`[cron] after ${source.name}`);
    checkHeapCritical(`[cron] after ${source.name}`);
  }

  if (!anySourceSucceeded) {
    console.error("[cron] No sources loaded. Keeping existing data.");
    abortRefresh(refreshDb);
    return;
  }

  // PEP refresh into the new database (non-fatal)
  try {
    const heapMB = getHeapMB();
    if (heapMB <= HEAP_PEP_SKIP_MB) {
      progress.state = "loading_peps";
      progress.currentSource = "PEP (Wikidata)";
      const pepCount = await ingestPep(refreshDb);
      totalEntities += pepCount;
      progress.pepCount = pepCount;
      console.log(`[cron] PEP refresh: ${pepCount} persons`);
    } else {
      console.warn(`[cron] Heap at ${heapMB.toFixed(0)}MB — skipping PEP refresh`);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[cron] PEP refresh failed (non-fatal): ${msg}`);
  }

  // Atomic swap: new database file → production
  console.log("[cron] Swapping database...");
  swapRefreshDb(refreshDb);

  progress.totalEntities = totalEntities;
  progress.sourcesLoaded = {};

  // Count per source in new database
  const rows = getDb()
    .prepare("SELECT source, COUNT(*) as cnt FROM entities GROUP BY source")
    .all() as Array<{ source: string; cnt: number }>;
  for (const r of rows) {
    progress.sourcesLoaded[r.source] = r.cnt;
  }

  progress.sanctionsReady = true;
  progress.state = "ready";
  shrinkMemory();
  tryGc();
  console.log(`[cron] Refresh complete: ${totalEntities} entities.`);
  logHeap("[cron] full refresh complete");
}

// CLI entry point
if (process.argv[1]?.endsWith("ingest.js") || process.argv[1]?.endsWith("ingest.ts")) {
  runInitialIngestion()
    .then(() => runPepIngestion())
    .then(() => {
      const total = (
        getDb().prepare("SELECT COUNT(*) as count FROM entities").get() as { count: number }
      ).count;
      console.log(`\n✓ Done. ${total} total entities indexed.`);
      process.exit(0);
    })
    .catch((err) => {
      console.error("Fatal error:", err);
      process.exit(1);
    });
}
