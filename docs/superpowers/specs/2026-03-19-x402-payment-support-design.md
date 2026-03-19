# x402 Payment Support Design

Add x402 as an alternative payment protocol alongside existing MPP/Tempo on paid endpoints.

## Context

Vigil/Argvs is a sanctions screening API (Hono) that currently accepts payment via MPP/Tempo (pathUSD). AI agents should also be able to pay via x402/Base (USDC), giving them a choice of payment protocol.

## Dependencies

**New packages:**
- `@x402/core` (^2.0.0) — transport-agnostic server/facilitator protocol primitives
- `@x402/evm` (^2.0.0) — EVM scheme (ExactEvmScheme for USDC via EIP-3009)

We use the core components directly rather than `@x402/hono`'s opinionated `paymentMiddleware`, because that middleware owns the full request lifecycle (returns 402 when no payment header is present), which would block MPP clients on the same route.

**Not needed:**
- `@coinbase/x402` — facilitator package, not needed since we use the public facilitator
- `@x402/hono` — its `paymentMiddleware` conflicts with dual-protocol routing

**Note on imports:** The x402 packages use sub-path exports. Exact import paths (e.g., `@x402/core/server`, `@x402/evm/exact/server`) will be confirmed during implementation by inspecting the installed package exports. The spec uses logical names; implementation may need path adjustments.

## Environment Variables

- `X402_WALLET_ADDRESS` — USDC receiving address on Base. Falls back to `TEMPO_WALLET_ADDRESS` if not set.
- `TESTNET` (existing) — when `true`, bypasses both MPP and x402 payment verification.

## Network Selection

- Production: `eip155:8453` (Base mainnet)
- TESTNET=true: `eip155:84532` (Base Sepolia) — though payments are bypassed in TESTNET mode, the network ID is still used in the self-discovery endpoint for informational purposes.

## Architecture

### New File: `src/x402.ts`

Encapsulates all x402 logic, keeping `index.ts` clean.

**Initialization:**
- `HTTPFacilitatorClient` pointing at `https://facilitator.x402.org`
- `x402ResourceServer` with the EVM exact scheme registered for the selected network
- Call `initialize()` at app startup to sync supported kinds from the facilitator. Handle initialization failure gracefully (log warning, x402 payments unavailable).

**Exported functions:**

```typescript
x402Charge(opts: { amount: string, description: string }): MiddlewareHandler
x402BatchCharge(): MiddlewareHandler
initX402(): Promise<void>
```

**`x402Charge` middleware behavior (for /api/screen):**
1. If TESTNET mode → call `next()` immediately (bypass)
2. Check for `X-PAYMENT` header on the request
3. If absent → call `next()` (fall through to MPP middleware)
4. If present → full x402 lifecycle:
   a. **Verify** the payment via x402ResourceServer
   b. If invalid → return 402 with x402-standard `PaymentRequirements` response body
   c. If valid → set `c.set("paymentHandled", true)` and call `next()`
   d. After route handler completes → **settle** the payment via x402ResourceServer

**`x402BatchCharge` middleware behavior (for /api/batch):**
Same as `x402Charge` but with dynamic pricing:
1. If TESTNET mode → call `next()` immediately
2. Check for `X-PAYMENT` header
3. If absent → call `next()` (fall through to MPP)
4. If present → peek at request body to count entities, calculate amount (`entityCount * 0.02`), then verify/settle as above. Store parsed body in `c.set("batchBody")` to avoid double-parsing.

### MPP Skip Coordination

When x402 successfully handles a payment, it sets `c.set("paymentHandled", true)`. The MPP middleware must be wrapped to check this flag before running:

```typescript
// Wrapper around mppx.charge() that skips if x402 already handled payment
function mppChargeIfNeeded(opts): MiddlewareHandler {
  const mppMiddleware = isTestnet ? noopCharge : mppx.charge(opts);
  return async (c, next) => {
    if (c.get("paymentHandled")) return next();
    return mppMiddleware(c, next);
  };
}
```

This is the key coordination mechanism between the two payment protocols.

### Route Wiring in `index.ts`

```typescript
app.post("/api/screen", x402Charge({...}), mppChargeIfNeeded({...}), handler)
app.post("/api/batch",  x402BatchCharge(), mppBatchChargeIfNeeded(), handler)
```

x402 middleware runs first:
- Client sends `X-PAYMENT` header → x402 verifies, sets flag, MPP skipped
- No `X-PAYMENT` header → x402 passes through, MPP handles its challenge/response
- Neither payment present → MPP returns its challenge (backward compatible)

### Self-Discovery Endpoint (GET /api)

Add a new `paymentMethods` field with structured objects. Keep the existing `payment` string array for backward compatibility.

```json
{
  "payment": ["MPP/Tempo (pathUSD)", "x402/Base (USDC)"],
  "paymentMethods": [
    {
      "protocol": "MPP/Tempo",
      "network": "Tempo",
      "currency": "pathUSD",
      "wallet": "0x91f3..."
    },
    {
      "protocol": "x402",
      "network": "Base (eip155:8453)",
      "currency": "USDC",
      "wallet": "0x91f3...",
      "facilitator": "https://facilitator.x402.org",
      "x402PaymentRequirements": {
        "scheme": "exact",
        "network": "eip155:8453"
      }
    }
  ]
}
```

In TESTNET mode, add `"testnet": true` to the top-level response so agents know payments are bypassed.

### TESTNET Behavior

- `x402Charge` / `x402BatchCharge` → call `next()` immediately (same pattern as MPP's `noopCharge`)
- MPP → uses existing `noopCharge` (unchanged)
- GET `/api` → lists both payment methods, adds `"testnet": true` flag

## Payment Protocol Detection

Detection is header-based, no client configuration needed:

| Header present | Protocol used |
|---------------|---------------|
| `X-PAYMENT` | x402 (verify → settle lifecycle) |
| MPP headers | MPP/Tempo |
| Neither | MPP challenge response (backward compatible) |

## Error Handling

- x402 verification failure → return HTTP 402 with x402-standard `PaymentRequirements` response body (generated by `buildPaymentRequirements()`)
- x402 settlement failure → log error, but do not fail the request (payment was verified, resource was served)
- x402 initialization failure → log warning at startup, x402 middleware falls through to MPP for all requests

## Files Changed

1. `src/x402.ts` (new) — x402 initialization, `x402Charge`, `x402BatchCharge` middleware, `initX402()`
2. `src/index.ts` (modified) — import x402, add `mppChargeIfNeeded` wrapper, wire into routes, update GET /api response with `paymentMethods`
3. `package.json` (modified) — add `@x402/core`, `@x402/evm`
