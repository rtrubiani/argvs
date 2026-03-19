import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { sources, type DataSource } from "./sources.js";

const DATA_DIR = join(process.cwd(), "data");
const MAX_RETRIES = 3;
const USER_AGENT = "Argvs Sanctions Screener/1.0.0";

async function downloadFile(
  url: string,
  filename: string,
  label: string
): Promise<string> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(
        `  [${attempt}/${MAX_RETRIES}] Downloading ${label}...`
      );
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT },
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText}`);
      }

      const data = await res.text();
      const filepath = join(DATA_DIR, filename);
      writeFileSync(filepath, data, "utf-8");
      console.log(
        `  ✓ ${label} saved (${(data.length / 1024 / 1024).toFixed(1)} MB)`
      );
      return filepath;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      console.warn(
        `  ✗ ${label} attempt ${attempt} failed: ${lastError.message}`
      );
      if (attempt < MAX_RETRIES) {
        const delay = 1000 * 2 ** (attempt - 1);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  throw new Error(
    `Failed to download ${label} after ${MAX_RETRIES} attempts: ${lastError?.message}`
  );
}

async function downloadWithRetry(source: DataSource): Promise<string> {
  const filepath = await downloadFile(source.url, source.filename, source.name);

  // Download extra files (e.g. OFAC SDN aliases + addresses)
  if (source.extraFiles) {
    for (const extra of source.extraFiles) {
      await downloadFile(extra.url, extra.filename, `${source.name} (${extra.filename})`);
    }
  }

  return filepath;
}

export interface DownloadResult {
  source: DataSource;
  filepath: string | null;
  error: string | null;
}

export async function downloadAllSources(): Promise<DownloadResult[]> {
  mkdirSync(DATA_DIR, { recursive: true });

  const results: DownloadResult[] = [];

  for (const source of sources) {
    try {
      const filepath = await downloadWithRetry(source);
      results.push({ source, filepath, error: null });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ✗ Skipping ${source.name}: ${message}`);
      results.push({ source, filepath: null, error: message });
    }
  }

  return results;
}
