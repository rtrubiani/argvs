import { downloadAllSources } from "./download.js";
import { parseSource } from "./parse.js";
import { resetDatabase, insertEntities } from "../db.js";

async function main() {
  console.log("=== Argvs Sanctions Data Ingestion ===\n");

  // Download
  console.log("Downloading sanctions lists...");
  const results = await downloadAllSources();

  const succeeded = results.filter((r) => r.filepath !== null);
  const failed = results.filter((r) => r.filepath === null);

  console.log(
    `\nDownloads: ${succeeded.length} succeeded, ${failed.length} failed\n`
  );

  if (succeeded.length === 0) {
    console.error("No sources downloaded. Aborting.");
    process.exit(1);
  }

  // Reset DB for fresh ingestion
  resetDatabase();

  // Parse and insert
  let totalEntities = 0;
  for (const result of succeeded) {
    console.log(`Parsing ${result.source.name}...`);
    try {
      const entities = parseSource(result.source, result.filepath!);
      console.log(`  → ${entities.length} entities`);
      insertEntities(entities);
      totalEntities += entities.length;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  ✗ Failed to parse ${result.source.name}: ${msg}`);
    }
  }

  console.log(`\n✓ Done. ${totalEntities} total entities indexed.`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
