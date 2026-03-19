import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import type { SanctionEntity } from "./ingest/parse.js";

const DB_PATH = join(process.cwd(), "data", "sanctions.db");

let db: Database.Database;

export function getDb(): Database.Database {
  if (!db) {
    mkdirSync(dirname(DB_PATH), { recursive: true });
    db = new Database(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.pragma("foreign_keys = ON");
  }
  return db;
}

export function initDatabase(): void {
  const d = getDb();

  d.exec(`
    CREATE TABLE IF NOT EXISTS entities (
      rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
      id        TEXT NOT NULL UNIQUE,
      source    TEXT NOT NULL,
      name      TEXT NOT NULL,
      aliases   TEXT NOT NULL DEFAULT '[]',
      type      TEXT NOT NULL DEFAULT 'unknown',
      programs  TEXT NOT NULL DEFAULT '[]',
      countries TEXT NOT NULL DEFAULT '[]',
      identifiers TEXT NOT NULL DEFAULT '[]',
      date_listed TEXT NOT NULL DEFAULT '',
      remarks   TEXT NOT NULL DEFAULT ''
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
      name,
      aliases,
      content=entities,
      content_rowid=rowid
    );

    CREATE TRIGGER IF NOT EXISTS entities_ai AFTER INSERT ON entities BEGIN
      INSERT INTO entities_fts(rowid, name, aliases)
      VALUES (new.rowid, new.name, new.aliases);
    END;

    CREATE TRIGGER IF NOT EXISTS entities_ad AFTER DELETE ON entities BEGIN
      INSERT INTO entities_fts(entities_fts, rowid, name, aliases)
      VALUES ('delete', old.rowid, old.name, old.aliases);
    END;

    CREATE TRIGGER IF NOT EXISTS entities_au AFTER UPDATE ON entities BEGIN
      INSERT INTO entities_fts(entities_fts, rowid, name, aliases)
      VALUES ('delete', old.rowid, old.name, old.aliases);
      INSERT INTO entities_fts(rowid, name, aliases)
      VALUES (new.rowid, new.name, new.aliases);
    END;
  `);

  console.log("Database initialized.");
}

// Reset database for fresh ingestion
export function resetDatabase(): void {
  const d = getDb();
  d.exec(`
    DROP TABLE IF EXISTS entities_fts;
    DROP TABLE IF EXISTS entities;
  `);
  initDatabase();
  console.log("Database reset.");
}

// ---------------------------------------------------------------------------
// Batch insert: inserts a batch of entities in a single transaction.
// Called with small batches (100 rows) from the streaming parsers.
// ---------------------------------------------------------------------------

const BATCH_SIZE = 100;

export function getInsertBatch() {
  const d = getDb();
  const insert = d.prepare(`
    INSERT OR REPLACE INTO entities (id, source, name, aliases, type, programs, countries, identifiers, date_listed, remarks)
    VALUES (@id, @source, @name, @aliases, @type, @programs, @countries, @identifiers, @date_listed, @remarks)
  `);

  const tx = d.transaction((rows: SanctionEntity[]) => {
    for (const row of rows) {
      insert.run({
        id: row.id,
        source: row.source,
        name: row.name,
        aliases: JSON.stringify(row.aliases),
        type: row.type,
        programs: JSON.stringify(row.programs),
        countries: JSON.stringify(row.countries),
        identifiers: JSON.stringify(row.identifiers),
        date_listed: row.date_listed,
        remarks: row.remarks,
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

// Legacy bulk insert (kept for compatibility)
export function insertEntities(entities: SanctionEntity[]): void {
  const batch = getInsertBatch();
  for (const e of entities) {
    batch.add(e);
  }
  batch.flush();
}

// Rebuild FTS5 index (call once after all data is loaded)
export function rebuildFtsIndex(): void {
  const d = getDb();
  d.exec(`INSERT INTO entities_fts(entities_fts) VALUES('rebuild')`);
  console.log("[db] FTS5 index rebuilt.");
}

// ---------------------------------------------------------------------------
// Temp table strategy for atomic refresh
// ---------------------------------------------------------------------------

export function createTempEntitiesTable(): void {
  const d = getDb();
  d.exec(`
    DROP TABLE IF EXISTS entities_new;
    CREATE TABLE entities_new (
      rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
      id        TEXT NOT NULL UNIQUE,
      source    TEXT NOT NULL,
      name      TEXT NOT NULL,
      aliases   TEXT NOT NULL DEFAULT '[]',
      type      TEXT NOT NULL DEFAULT 'unknown',
      programs  TEXT NOT NULL DEFAULT '[]',
      countries TEXT NOT NULL DEFAULT '[]',
      identifiers TEXT NOT NULL DEFAULT '[]',
      date_listed TEXT NOT NULL DEFAULT '',
      remarks   TEXT NOT NULL DEFAULT ''
    );
  `);
}

export function getInsertBatchForTable(tableName: string) {
  const d = getDb();
  const insert = d.prepare(`
    INSERT OR REPLACE INTO ${tableName} (id, source, name, aliases, type, programs, countries, identifiers, date_listed, remarks)
    VALUES (@id, @source, @name, @aliases, @type, @programs, @countries, @identifiers, @date_listed, @remarks)
  `);

  const tx = d.transaction((rows: SanctionEntity[]) => {
    for (const row of rows) {
      insert.run({
        id: row.id,
        source: row.source,
        name: row.name,
        aliases: JSON.stringify(row.aliases),
        type: row.type,
        programs: JSON.stringify(row.programs),
        countries: JSON.stringify(row.countries),
        identifiers: JSON.stringify(row.identifiers),
        date_listed: row.date_listed,
        remarks: row.remarks,
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

export function swapTempTable(): void {
  const d = getDb();
  d.exec(`
    DROP TABLE IF EXISTS entities_fts;
    DROP TABLE IF EXISTS entities_old;
    ALTER TABLE entities RENAME TO entities_old;
    ALTER TABLE entities_new RENAME TO entities;
    DROP TABLE IF EXISTS entities_old;

    CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
      name,
      aliases,
      content=entities,
      content_rowid=rowid
    );

    CREATE TRIGGER IF NOT EXISTS entities_ai AFTER INSERT ON entities BEGIN
      INSERT INTO entities_fts(rowid, name, aliases)
      VALUES (new.rowid, new.name, new.aliases);
    END;

    CREATE TRIGGER IF NOT EXISTS entities_ad AFTER DELETE ON entities BEGIN
      INSERT INTO entities_fts(entities_fts, rowid, name, aliases)
      VALUES ('delete', old.rowid, old.name, old.aliases);
    END;

    CREATE TRIGGER IF NOT EXISTS entities_au AFTER UPDATE ON entities BEGIN
      INSERT INTO entities_fts(entities_fts, rowid, name, aliases)
      VALUES ('delete', old.rowid, old.name, old.aliases);
      INSERT INTO entities_fts(rowid, name, aliases)
      VALUES (new.rowid, new.name, new.aliases);
    END;

    INSERT INTO entities_fts(entities_fts) VALUES('rebuild');
  `);
  console.log("[db] Table swap complete, FTS rebuilt.");
}

// ---------------------------------------------------------------------------
// Search
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
  query: string,
  limit: number = 10
): SearchResult[] {
  const d = getDb();

  const stmt = d.prepare(`
    SELECT
      e.*,
      entities_fts.rank AS score
    FROM entities_fts
    JOIN entities e ON e.rowid = entities_fts.rowid
    WHERE entities_fts MATCH @query
    ORDER BY entities_fts.rank
    LIMIT @limit
  `);

  const rows = stmt.all({ query, limit }) as any[];

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
    score: row.score,
  }));
}
