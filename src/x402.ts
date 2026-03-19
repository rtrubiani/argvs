import type { MiddlewareHandler } from "hono";
import { x402ResourceServer } from "@x402/core/server";
import { HTTPFacilitatorClient, x402HTTPResourceServer } from "@x402/core/http";
import { ExactEvmScheme } from "@x402/evm/exact/server";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const TESTNET = process.env.TESTNET === "true";
const WALLET =
  process.env.X402_WALLET_ADDRESS ??
  process.env.TEMPO_WALLET_ADDRESS ??
  "0x91f34413C72843dC68e185A1E0aBF8f7638702B0";

const FACILITATOR_URL = "https://facilitator.x402.org";
export const X402_NETWORK: `${string}:${string}` = TESTNET
  ? "eip155:84532"
  : "eip155:8453";

// ---------------------------------------------------------------------------
// Server instance (initialized lazily via initX402)
// ---------------------------------------------------------------------------
let httpServer: x402HTTPResourceServer | null = null;

export async function initX402(): Promise<void> {
  try {
    const facilitator = new HTTPFacilitatorClient({ url: FACILITATOR_URL });
    const resourceServer = new x402ResourceServer(facilitator);
    resourceServer.register(X402_NETWORK, new ExactEvmScheme());

    // We build the HTTP server with a single route config that covers both endpoints.
    // The actual route matching and dynamic pricing is handled by our custom middleware,
    // so this config just provides the scheme/network info for payment processing.
    const routes = {
      "POST /api/screen": {
        accepts: {
          scheme: "exact",
          payTo: WALLET,
          price: "$0.03",
          network: X402_NETWORK,
        },
        description: "Screen a single name against sanctions lists",
      },
      "POST /api/batch": {
        accepts: {
          scheme: "exact",
          payTo: WALLET,
          price: "$0.02",
          network: X402_NETWORK,
        },
        description: "Batch screen names against sanctions lists",
      },
    };

    httpServer = new x402HTTPResourceServer(resourceServer, routes);

    // Timeout initialization to prevent hanging on network-restricted environments
    await Promise.race([
      httpServer.initialize(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("timed out after 10s")), 10_000)
      ),
    ]);
    console.log(`x402 payment support initialized (network: ${X402_NETWORK})`);
  } catch (err) {
    console.warn(
      "[x402] Initialization failed, x402 payments unavailable:",
      err instanceof Error ? err.message : err
    );
    httpServer = null;
  }
}

// ---------------------------------------------------------------------------
// Middleware: x402Charge (fixed price, for /api/screen)
// ---------------------------------------------------------------------------
export function x402Charge(opts: {
  amount: string;
  description: string;
}): MiddlewareHandler {
  return async (c, next) => {
    if (TESTNET) return next();

    const paymentHeader =
      c.req.header("X-PAYMENT") ?? c.req.header("x-payment");

    if (!paymentHeader) return next(); // no x402 header → fall through to MPP

    if (!httpServer) {
      return c.json({ error: "x402 payments not available" }, 503);
    }

    const adapter = {
      getHeader: (name: string) => c.req.header(name),
      getMethod: () => c.req.method,
      getPath: () => new URL(c.req.url).pathname,
      getUrl: () => c.req.url,
      getAcceptHeader: () => c.req.header("accept") ?? "",
      getUserAgent: () => c.req.header("user-agent") ?? "",
    };

    const context = {
      adapter,
      path: adapter.getPath(),
      method: adapter.getMethod(),
      paymentHeader,
    };

    const result = await httpServer.processHTTPRequest(context);

    if (result.type === "payment-error") {
      return c.json(result.response.body ?? {}, result.response.status as any);
    }

    if (result.type === "payment-verified") {
      // Mark payment as handled so MPP middleware skips
      c.set("paymentHandled" as never, true as never);
      c.set("paymentMethod" as never, "x402" as never);

      await next();

      // Settle after response (fire-and-forget to avoid blocking)
      httpServer
        .processSettlement(
          result.paymentPayload,
          result.paymentRequirements,
          result.declaredExtensions
        )
        .catch((err) =>
          console.error(
            "[x402] Settlement failed:",
            err instanceof Error ? err.message : err
          )
        );

      return;
    }

    // no-payment-required (shouldn't happen for our routes, but handle gracefully)
    return next();
  };
}

// ---------------------------------------------------------------------------
// Middleware: x402BatchCharge (dynamic price, for /api/batch)
// ---------------------------------------------------------------------------
export function x402BatchCharge(): MiddlewareHandler {
  return async (c, next) => {
    if (TESTNET) return next();

    const paymentHeader =
      c.req.header("X-PAYMENT") ?? c.req.header("x-payment");

    if (!paymentHeader) return next(); // fall through to MPP

    if (!httpServer) {
      return c.json({ error: "x402 payments not available" }, 503);
    }

    // Peek at body for dynamic pricing (same pattern as MPP batch)
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
      return c.json({ error: "entities array must not be empty" }, 400);
    }

    // Store parsed body to avoid double-parsing
    c.set("batchBody" as never, body as never);

    const adapter = {
      getHeader: (name: string) => c.req.header(name),
      getMethod: () => c.req.method,
      getPath: () => new URL(c.req.url).pathname,
      getUrl: () => c.req.url,
      getAcceptHeader: () => c.req.header("accept") ?? "",
      getUserAgent: () => c.req.header("user-agent") ?? "",
    };

    const context = {
      adapter,
      path: adapter.getPath(),
      method: adapter.getMethod(),
      paymentHeader,
    };

    const result = await httpServer.processHTTPRequest(context);

    if (result.type === "payment-error") {
      return c.json(result.response.body ?? {}, result.response.status as any);
    }

    if (result.type === "payment-verified") {
      c.set("paymentHandled" as never, true as never);
      c.set("paymentMethod" as never, "x402" as never);

      await next();

      httpServer
        .processSettlement(
          result.paymentPayload,
          result.paymentRequirements,
          result.declaredExtensions
        )
        .catch((err) =>
          console.error(
            "[x402] Settlement failed:",
            err instanceof Error ? err.message : err
          )
        );

      return;
    }

    return next();
  };
}

// ---------------------------------------------------------------------------
// Exports for self-discovery
// ---------------------------------------------------------------------------
export const X402_WALLET = WALLET;
export const X402_FACILITATOR = FACILITATOR_URL;
