import Database from "better-sqlite3";
import { mkdirSync, unlinkSync, renameSync } from "node:fs";
import { dirname, join } from "node:path";
import type { SanctionEntity } from "./ingest/parse.js";

const DB_PATH = join(process.cwd(), "data", "sanctions.db");
const DB_NEW_PATH = join(process.cwd(), "data", "sanctions_new.db");
const BATCH_SIZE = 100;

let db: Database.Database;

// ---------------------------------------------------------------------------
// Build search_text: lowercase name + all aliases, joined with spaces.
// Uses array.join() to avoid intermediate string concatenation copies.
// ---------------------------------------------------------------------------
function buildSearchText(name: string, aliases: string[]): string {
  const parts: string[] = [name.toLowerCase()];
  for (const a of aliases) {
    parts.push(a.toLowerCase());
  }
  return parts.join(" ");
}

// ---------------------------------------------------------------------------
// Memory-minimizing pragmas applied to every database connection
// ---------------------------------------------------------------------------
function applyPragmas(d: Database.Database): void {
  d.pragma("page_size = 4096");
  d.pragma("cache_size = -1000"); // 1MB only
  d.pragma("mmap_size = 0");
  d.pragma("journal_mode = DELETE"); // not WAL — WAL keeps a second copy
  d.pragma("temp_store = FILE"); // force temp tables to disk, not memory
}

function createSchema(d: Database.Database): void {
  d.exec(`
    CREATE TABLE IF NOT EXISTS entities (
      id          TEXT NOT NULL UNIQUE,
      source      TEXT NOT NULL,
      name        TEXT NOT NULL,
      aliases     TEXT NOT NULL DEFAULT '[]',
      type        TEXT NOT NULL DEFAULT 'unknown',
      programs    TEXT NOT NULL DEFAULT '[]',
      countries   TEXT NOT NULL DEFAULT '[]',
      identifiers TEXT NOT NULL DEFAULT '[]',
      date_listed TEXT NOT NULL DEFAULT '',
      remarks     TEXT NOT NULL DEFAULT '',
      search_text TEXT NOT NULL DEFAULT ''
    );
  `);
}

// ---------------------------------------------------------------------------
// Database lifecycle
// ---------------------------------------------------------------------------

export function getDb(): Database.Database {
  if (!db) {
    mkdirSync(dirname(DB_PATH), { recursive: true });
    db = new Database(DB_PATH);
    applyPragmas(db);
    db.pragma("synchronous = NORMAL");
  }
  return db;
}

export function initDatabase(): void {
  const d = getDb();
  createSchema(d);
  console.log("Database initialized.");
}

export function resetDatabase(): void {
  const d = getDb();
  d.exec("DROP TABLE IF EXISTS entities");
  createSchema(d);
  d.pragma("synchronous = OFF"); // fast during ingestion
  console.log("Database reset.");
}

export function setRuntimePragmas(): void {
  getDb().pragma("synchronous = NORMAL");
}

export function shrinkMemory(): void {
  getDb().pragma("shrink_memory");
}

// ---------------------------------------------------------------------------
// Batch insert — computes search_text inline during INSERT
// ---------------------------------------------------------------------------

export function getInsertBatch(targetDb?: Database.Database) {
  const d = targetDb ?? getDb();
  const insert = d.prepare(`
    INSERT OR REPLACE INTO entities
      (id, source, name, aliases, type, programs, countries, identifiers, date_listed, remarks, search_text)
    VALUES
      (@id, @source, @name, @aliases, @type, @programs, @countries, @identifiers, @date_listed, @remarks, @search_text)
  `);

  const tx = d.transaction((rows: SanctionEntity[]) => {
    for (const row of rows) {
      const aliasesJson = JSON.stringify(row.aliases);
      insert.run({
        id: row.id,
        source: row.source,
        name: row.name,
        aliases: aliasesJson,
        type: row.type,
        programs: JSON.stringify(row.programs),
        countries: JSON.stringify(row.countries),
        identifiers: JSON.stringify(row.identifiers),
        date_listed: row.date_listed,
        remarks: row.remarks,
        search_text: buildSearchText(row.name, row.aliases),
      });
    }
  });

  let buffer: SanctionEntity[] = [];
  let totalInserted = 0;

  return {
    add(entity: SanctionEntity): void {
      buffer.push(entity);
      if (buffer.length >= BATCH_SIZE) {
        tx(buffer);
        totalInserted += buffer.length;
        buffer = [];
      }
    },
    flush(): number {
      if (buffer.length > 0) {
        tx(buffer);
        totalInserted += buffer.length;
        buffer = [];
      }
      return totalInserted;
    },
    get count() {
      return totalInserted + buffer.length;
    },
  };
}

// ---------------------------------------------------------------------------
// Search: LIKE on search_text column — no FTS5, near-zero memory overhead.
// 80K rows with LIKE still completes in <100ms on SQLite.
// The matching engine (Levenshtein + token overlap) does the real scoring.
// ---------------------------------------------------------------------------

export interface SearchResult {
  id: string;
  source: string;
  name: string;
  aliases: string[];
  type: string;
  programs: string[];
  countries: string[];
  identifiers: string[];
  date_listed: string;
  remarks: string;
  score: number;
}

export function searchEntities(
  tokens: string[],
  limit: number = 30
): SearchResult[] {
  if (tokens.length === 0) return [];

  const d = getDb();
  const conditions = tokens.map((_, i) => `search_text LIKE @t${i}`);
  const sql = `SELECT * FROM entities WHERE ${conditions.join(" AND ")} LIMIT @limit`;

  const stmt = d.prepare(sql);
  const params: Record<string, string | number> = { limit };
  for (let i = 0; i < tokens.length; i++) {
    params[`t${i}`] = `%${tokens[i]}%`;
  }

  const rows = stmt.all(params) as any[];
  return rows.map((row) => ({
    id: row.id,
    source: row.source,
    name: row.name,
    aliases: JSON.parse(row.aliases),
    type: row.type,
    programs: JSON.parse(row.programs),
    countries: JSON.parse(row.countries),
    identifiers: JSON.parse(row.identifiers),
    date_listed: row.date_listed,
    remarks: row.remarks,
    score: 0,
  }));
}

// ---------------------------------------------------------------------------
// Daily refresh: build into a separate database file for safe swap.
// Never wipe the working database before new data is ready.
// ---------------------------------------------------------------------------

export function createRefreshDb(): Database.Database {
  mkdirSync(dirname(DB_NEW_PATH), { recursive: true });
  try { unlinkSync(DB_NEW_PATH); } catch {}
  const d = new Database(DB_NEW_PATH);
  applyPragmas(d);
  d.pragma("synchronous = OFF");
  createSchema(d);
  return d;
}

export function swapRefreshDb(refreshDb: Database.Database): void {
  refreshDb.close();
  db.close();
  renameSync(DB_NEW_PATH, DB_PATH);
  // Reopen at production path
  db = new Database(DB_PATH);
  applyPragmas(db);
  db.pragma("synchronous = NORMAL");
  console.log("[db] Database swap complete.");
}

export function abortRefresh(refreshDb: Database.Database): void {
  try { refreshDb.close(); } catch {}
  try { unlinkSync(DB_NEW_PATH); } catch {}
  console.log("[db] Refresh aborted, keeping existing database.");
}
