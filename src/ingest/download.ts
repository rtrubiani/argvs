import { mkdirSync, createWriteStream, unlinkSync, existsSync, statSync } from "node:fs";
import { join } from "node:path";
import { sources, type DataSource } from "./sources.js";

const DATA_DIR = join(process.cwd(), "data");
const MAX_RETRIES = 3;
const USER_AGENT = "Argvs Sanctions Screener/1.0.0";

// Stream download to disk — never hold the whole file in memory
async function downloadFile(
  url: string,
  filename: string,
  label: string
): Promise<string> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`  [${attempt}/${MAX_RETRIES}] Downloading ${label}...`);
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT },
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText}`);
      }

      const filepath = join(DATA_DIR, filename);
      const fileStream = createWriteStream(filepath);

      // Stream the response body directly to disk
      const reader = res.body?.getReader();
      if (!reader) throw new Error("No response body");

      let totalBytes = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        totalBytes += value.byteLength;
        const canWrite = fileStream.write(value);
        if (!canWrite) {
          await new Promise<void>((resolve) => fileStream.once("drain", resolve));
        }
      }

      await new Promise<void>((resolve, reject) => {
        fileStream.end(() => resolve());
        fileStream.on("error", reject);
      });

      console.log(`  ✓ ${label} saved (${(totalBytes / 1024 / 1024).toFixed(1)} MB)`);
      return filepath;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      console.warn(`  ✗ ${label} attempt ${attempt} failed: ${lastError.message}`);
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

  if (source.extraFiles) {
    for (const extra of source.extraFiles) {
      await downloadFile(extra.url, extra.filename, `${source.name} (${extra.filename})`);
    }
  }

  return filepath;
}

export function deleteFile(filepath: string): void {
  try {
    if (existsSync(filepath)) {
      const size = statSync(filepath).size;
      unlinkSync(filepath);
      const sizeMB = (size / 1024 / 1024).toFixed(1);
      console.log(`  Deleted ${filepath} (${sizeMB}MB freed)`);
    }
  } catch {
    // Best effort
  }
}

export function deleteSourceFiles(source: DataSource): void {
  deleteFile(join(DATA_DIR, source.filename));
  if (source.extraFiles) {
    for (const extra of source.extraFiles) {
      deleteFile(join(DATA_DIR, extra.filename));
    }
  }
}

export interface DownloadResult {
  source: DataSource;
  filepath: string | null;
  error: string | null;
}

export async function downloadSource(source: DataSource): Promise<DownloadResult> {
  mkdirSync(DATA_DIR, { recursive: true });
  try {
    const filepath = await downloadWithRetry(source);
    return { source, filepath, error: null };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`  ✗ Skipping ${source.name}: ${message}`);
    return { source, filepath: null, error: message };
  }
}

export async function downloadAllSources(): Promise<DownloadResult[]> {
  mkdirSync(DATA_DIR, { recursive: true });
  const results: DownloadResult[] = [];
  for (const source of sources) {
    results.push(await downloadSource(source));
  }
  return results;
}
