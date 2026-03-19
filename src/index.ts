import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomBytes } from "node:crypto";
import cron from "node-cron";
import { Mppx, tempo } from "mppx/hono";
import { initDatabase, getDb } from "./db.js";
import { screenEntity, batchScreen } from "./match.js";
import { downloadAllSources } from "./ingest/download.js";
import { parseSource } from "./ingest/parse.js";
import {
  initX402,
  x402Charge,
  x402BatchCharge,
  X402_NETWORK,
  X402_WALLET,
  X402_FACILITATOR,
} from "./x402.js";
import { notifyScreen, notifyBatch } from "./telegram.js";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT ?? "3000", 10);
const WALLET =
  process.env.TEMPO_WALLET_ADDRESS ??
  "0x91f34413C72843dC68e185A1E0aBF8f7638702B0";
const SECRET_KEY =
  process.env.MPP_SECRET_KEY ?? randomBytes(32).toString("base64");

// pathUSD on Tempo
const PATHUSD = "0x20c0000000000000000000000000000000000000";
const TESTNET = process.env.TESTNET === "true";

// ---------------------------------------------------------------------------
// MPP payment handler
// ---------------------------------------------------------------------------
const mppx = Mppx.create({
  methods: [
    tempo.charge({
      currency: PATHUSD as `0x${string}`,
      recipient: WALLET as `0x${string}`,
    }),
  ],
  secretKey: SECRET_KEY,
});

// In testnet mode, skip payment and pass through
const noopCharge = (_opts: { amount: string; description: string }) =>
  async (_c: any, next: any) => next();

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------
const app = new Hono();

app.use("*", cors({ origin: "*" }));

// --- Free endpoints -------------------------------------------------------

app.get("/api", (c) =>
  c.json({
    name: "Argvs",
    description:
      "Instant sanctions screening for AI agents. Checks OFAC, EU, UN, UK lists.",
    version: "1.0.0",
    endpoints: [
      {
        path: "/api/screen",
        method: "POST",
        price: "$0.03",
        description: "Screen a single name against all sanctions lists",
      },
      {
        path: "/api/batch",
        method: "POST",
        price: "$0.02 per entity",
        description: "Screen multiple names in a single request (max 100)",
      },
      {
        path: "/api/status",
        method: "GET",
        price: "free",
        description: "Check data freshness and list status",
      },
      {
        path: "/api",
        method: "GET",
        price: "free",
        description: "Service discovery and documentation",
      },
    ],
    payment: ["MPP/Tempo (pathUSD)", "x402/Base (USDC)"],
    paymentMethods: [
      {
        protocol: "MPP/Tempo",
        network: "Tempo",
        currency: "pathUSD",
        wallet: WALLET,
      },
      {
        protocol: "x402",
        network: `Base (${X402_NETWORK})`,
        currency: "USDC",
        wallet: X402_WALLET,
        facilitator: X402_FACILITATOR,
        scheme: "exact",
      },
    ],
    ...(TESTNET && { testnet: true }),
    sources: [
      "OFAC SDN",
      "OFAC Consolidated",
      "EU Consolidated",
      "UN SCSC",
      "UK HMT",
    ],
  })
);

app.get("/api/status", (c) => {
  const db = getDb();
  const sources = db
    .prepare(
      `SELECT source, COUNT(*) as entity_count, MAX(date_listed) as last_updated
       FROM entities GROUP BY source`
    )
    .all() as { source: string; entity_count: number; last_updated: string }[];

  const total = db
    .prepare("SELECT COUNT(*) as count FROM entities")
    .get() as { count: number };

  const lists: Record<
    string,
    { last_updated: string; entity_count: number }
  > = {};
  for (const s of sources) {
    lists[s.source] = {
      last_updated: s.last_updated,
      entity_count: s.entity_count,
    };
  }

  return c.json({
    lists,
    total_entities: total.count,
    matching_method: "FTS5 + Levenshtein + token overlap",
    update_frequency: "daily",
  });
});

app.get("/.well-known/mcp.json", async (c) => {
  const { readFileSync } = await import("node:fs");
  const { join } = await import("node:path");
  const mcpPath = join(process.cwd(), ".well-known", "mcp.json");
  const content = readFileSync(mcpPath, "utf-8");
  return c.json(JSON.parse(content));
});

// --- Paid endpoints -------------------------------------------------------

app.post(
  "/api/screen",
  x402Charge({ amount: "0.03", description: "Argvs: single sanctions screen" }),
  async (c, next) => {
    if (c.get("paymentHandled" as never)) return next();
    return (TESTNET ? noopCharge : mppx.charge)({
      amount: "0.03",
      description: "Argvs: single sanctions screen",
    })(c, next);
  },
  async (c) => {
    const body = await c.req.json().catch(() => null);
    if (!body?.name || typeof body.name !== "string") {
      return c.json({ error: "Missing required field: name" }, 400);
    }
    const result = screenEntity({
      name: body.name,
      type: body.type,
      country: body.country,
    });
    const payment = (c.get("paymentMethod" as never) as string) ?? "MPP";
    notifyScreen({
      query: body.name,
      riskLevel: result.risk_level,
      confidence: result.matches?.[0]?.confidence ?? 0,
      endpoint: "/api/screen",
      payment,
    });
    return c.json(result);
  }
);

app.post(
  "/api/batch",
  x402BatchCharge(),
  async (c, next) => {
    // If x402 already handled payment, skip MPP
    if (c.get("paymentHandled" as never)) return next();
    // Dynamic pricing: peek at entity count to set the amount
    const body = await c.req.json().catch(() => null);
    if (!body?.entities || !Array.isArray(body.entities)) {
      return c.json(
        { error: "Missing required field: entities (array)" },
        400
      );
    }
    if (body.entities.length > 100) {
      return c.json(
        { error: "Maximum 100 entities per batch request" },
        413
      );
    }
    if (body.entities.length === 0) {
      return c.json(
        { error: "entities array must not be empty" },
        400
      );
    }
    // Store parsed body for downstream handler
    c.set("batchBody" as never, body as never);
    // Apply payment middleware with dynamic amount
    const amount = (body.entities.length * 0.02).toFixed(2);
    if (TESTNET) return next();
    const paymentMiddleware = mppx.charge({
      amount,
      description: `Argvs: batch screen (${body.entities.length} entities)`,
    });
    return paymentMiddleware(c, next);
  },
  async (c) => {
    const body = c.get("batchBody" as never) as {
      entities: Array<{ name: string; type?: string; country?: string }>;
    };
    const results = batchScreen(body.entities);
    const payment = (c.get("paymentMethod" as never) as string) ?? "MPP";
    const flagged = Array.isArray(results)
      ? results.filter((r: any) => r.risk_level !== "clear").length
      : 0;
    notifyBatch({
      entityCount: body.entities.length,
      flaggedCount: flagged,
      payment,
    });
    return c.json(results);
  }
);

// ---------------------------------------------------------------------------
// Data refresh (daily at 06:00 UTC)
// ---------------------------------------------------------------------------
async function refreshData() {
  console.log("[cron] Starting daily sanctions data refresh...");
  try {
    const results = await downloadAllSources();
    const succeeded = results.filter((r) => r.filepath !== null);
    if (succeeded.length === 0) {
      console.error("[cron] No sources downloaded. Skipping refresh.");
      return;
    }
    const { resetDatabase, insertEntities } = await import("./db.js");
    resetDatabase();
    let total = 0;
    for (const result of succeeded) {
      try {
        const entities = parseSource(result.source, result.filepath!);
        insertEntities(entities);
        total += entities.length;
      } catch (err) {
        console.error(
          `[cron] Failed to parse ${result.source.name}:`,
          err instanceof Error ? err.message : err
        );
      }
    }
    console.log(`[cron] Refresh complete. ${total} entities indexed.`);
  } catch (err) {
    console.error("[cron] Refresh failed:", err);
  }
}

cron.schedule("0 6 * * *", refreshData, { timezone: "UTC" });

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------
initDatabase();

const totalEntities = (
  getDb().prepare("SELECT COUNT(*) as count FROM entities").get() as {
    count: number;
  }
).count;

console.log(`Loaded ${totalEntities} entities`);
if (TESTNET) console.log("⚠ TESTNET mode: payments disabled");

// Initialize x402 (async, non-blocking — server starts immediately)
initX402().catch(() => {});

console.log(`Argvs running on port ${PORT}`);

serve({ fetch: app.fetch, port: PORT });
