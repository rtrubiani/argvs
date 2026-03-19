import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomBytes } from "node:crypto";
import cron from "node-cron";
import { Mppx, tempo } from "mppx/hono";
import { initDatabase, getDb } from "./db.js";
import { screenEntity, batchScreen } from "./match.js";
import {
  runInitialIngestion,
  runPepIngestion,
  runDailyRefresh,
  progress,
} from "./ingest/ingest.js";
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
// Runtime state
// ---------------------------------------------------------------------------
let dataReady = false;

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
      "Instant sanctions and PEP screening for AI agents. Checks OFAC, EU, UN, UK lists and Politically Exposed Persons (PEP) via Wikidata.",
    version: "1.0.0",
    endpoints: [
      {
        path: "/api/screen",
        method: "POST",
        price: "$0.03",
        description: "Screen a single name against all sanctions lists and PEP database",
      },
      {
        path: "/api/batch",
        method: "POST",
        price: "$0.02 per entity",
        description: "Screen multiple names against sanctions lists and PEP database (max 100)",
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
      "PEP (Wikidata)",
    ],
  })
);

app.get("/api/status", (c) => {
  // Always show real-time progress
  if (!dataReady) {
    return c.json({
      state: progress.state,
      message: progress.state === "idle" ? "Waiting to start" : "Data loading in progress",
      currentSource: progress.currentSource,
      sourcesLoaded: progress.sourcesLoaded,
      totalEntities: progress.totalEntities,
      errors: progress.errors,
      heap_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      matching_method: "FTS5 + Levenshtein + token overlap",
      update_frequency: "daily",
    });
  }

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
    state: "ready",
    lists,
    total_entities: total.count,
    heap_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
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

// Block screening endpoints until data is loaded — return 503
app.use("/api/screen", async (c, next) => {
  if (!dataReady) {
    return c.json(
      { error: "Data loading in progress, try again shortly" },
      503
    );
  }
  return next();
});
app.use("/api/batch", async (c, next) => {
  if (!dataReady) {
    return c.json(
      { error: "Data loading in progress, try again shortly" },
      503
    );
  }
  return next();
});

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
    if (c.get("paymentHandled" as never)) return next();
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
    c.set("batchBody" as never, body as never);
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
cron.schedule("0 6 * * *", () => {
  runDailyRefresh().catch((err) => {
    console.error("[cron] Refresh failed:", err);
  });
}, { timezone: "UTC" });

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------
initDatabase();

if (TESTNET) console.log("⚠ TESTNET mode: payments disabled");

// Initialize x402 (async, non-blocking — server starts immediately)
initX402().catch((err) => {
  console.warn("[x402] Initialization failed (non-fatal):", err instanceof Error ? err.message : err);
});

console.log(`Argvs running on port ${PORT} (data loading in background...)`);

serve({ fetch: app.fetch, port: PORT });

// Run initial data ingestion in the background after server starts
(async () => {
  try {
    // Phase 1: Load sanctions lists (streaming)
    await runInitialIngestion();
    dataReady = true;

    const total = (
      getDb().prepare("SELECT COUNT(*) as count FROM entities").get() as {
        count: number;
      }
    ).count;
    console.log(`[startup] Sanctions ready. ${total} entities loaded. Accepting requests.`);

    // Phase 2: Load PEPs in background (server already accepting requests)
    await runPepIngestion();

    const finalTotal = (
      getDb().prepare("SELECT COUNT(*) as count FROM entities").get() as {
        count: number;
      }
    ).count;
    console.log(`[startup] Full data ready. ${finalTotal} entities total.`);
  } catch (err) {
    console.error("[startup] Initial ingestion failed:", err);
    // Mark ready anyway if we have any data
    const count = (
      getDb().prepare("SELECT COUNT(*) as count FROM entities").get() as {
        count: number;
      }
    ).count;
    if (count > 0) {
      dataReady = true;
      console.log(`[startup] Partial data available: ${count} entities. Server ready.`);
    }
  }
})();
