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
import { streamParseSource, tryGc, logHeap } from "./parse.js";
import { ingestPep } from "./pep.js";
import {
  resetDatabase,
  getInsertBatch,
  rebuildFtsIndex,
  createTempEntitiesTable,
  getInsertBatchForTable,
  swapTempTable,
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

// ---------------------------------------------------------------------------
// Initial ingestion: download → stream-parse → insert → delete, one at a time
// ---------------------------------------------------------------------------

export async function runInitialIngestion(): Promise<void> {
  console.log("=== Argvs Sanctions Data Ingestion (Streaming) ===\n");

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
  }

  // 5. Build FTS5 index once at the end
  console.log("\nBuilding FTS5 search index...");
  rebuildFtsIndex();

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

  try {
    const pepCount = await ingestPep();
    progress.pepCount = pepCount;
    progress.totalEntities += pepCount;

    // Rebuild FTS to include PEP entries
    if (pepCount > 0) {
      rebuildFtsIndex();
    }

    console.log(`\n✓ PEP ingestion complete: ${pepCount} persons`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[pep] PEP ingestion failed (non-fatal): ${msg}`);
    progress.errors.push(`PEP: ${msg}`);
  }

  progress.currentSource = null;
  progress.state = "ready";
  logHeap("after PEP ingestion");
}

// ---------------------------------------------------------------------------
// Daily refresh: uses temp table strategy for atomic swap
// ---------------------------------------------------------------------------

export async function runDailyRefresh(): Promise<void> {
  console.log("[cron] Starting daily sanctions + PEP data refresh...");

  // Create temp table for new data
  createTempEntitiesTable();

  let totalEntities = 0;
  let anySourceSucceeded = false;

  for (const source of sources) {
    progress.currentSource = source.name;
    progress.state = "downloading";
    console.log(`\n[cron] --- ${source.name} ---`);

    const result = await downloadSource(source);
    if (!result.filepath) {
      progress.errors.push(`[cron] ${source.name}: ${result.error}`);
      continue;
    }

    progress.state = "parsing";
    try {
      const batch = getInsertBatchForTable("entities_new");
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
  }

  if (!anySourceSucceeded) {
    console.error("[cron] No sources loaded. Keeping existing data.");
    getDb().exec("DROP TABLE IF EXISTS entities_new");
    return;
  }

  // Atomic swap: rename entities_new → entities, rebuild FTS
  console.log("[cron] Swapping tables...");
  swapTempTable();
  progress.totalEntities = totalEntities;
  progress.sourcesLoaded = {};

  // Count per source in new table
  const rows = getDb()
    .prepare("SELECT source, COUNT(*) as cnt FROM entities GROUP BY source")
    .all() as Array<{ source: string; cnt: number }>;
  for (const r of rows) {
    progress.sourcesLoaded[r.source] = r.cnt;
  }

  progress.sanctionsReady = true;
  progress.state = "ready";
  console.log(`[cron] Sanctions refreshed: ${totalEntities} entities.`);
  logHeap("[cron] sanctions refresh complete");

  // PEP refresh (non-fatal)
  try {
    progress.state = "loading_peps";
    progress.currentSource = "PEP (Wikidata)";
    const pepCount = await ingestPep();
    progress.pepCount = pepCount;
    progress.totalEntities += pepCount;
    if (pepCount > 0) rebuildFtsIndex();
    console.log(`[cron] PEP refresh: ${pepCount} persons`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[cron] PEP refresh failed (non-fatal): ${msg}`);
  }

  progress.currentSource = null;
  progress.state = "ready";
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
