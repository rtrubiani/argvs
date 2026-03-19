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
- `x402ResourceServer` with `ExactEvmScheme` registered for the selected network

**Exported function:**
```typescript
x402Charge(opts: { amount: string, description: string }): MiddlewareHandler
```

Returns a Hono middleware handler with this behavior:
1. If TESTNET mode → call `next()` immediately (bypass)
2. Check for `X-PAYMENT` header on the request
3. If absent → call `next()` (fall through to MPP middleware)
4. If present → use x402ResourceServer to verify the payment
   - Valid → call `next()` to proceed to the route handler
   - Invalid → return 402 with error details

### Route Wiring in `index.ts`

```typescript
app.post("/api/screen", x402Charge({...}), mppCharge({...}), handler)
app.post("/api/batch",  x402Charge({...}), mppCharge({...}), handler)
```

x402 middleware runs first:
- Client sends `X-PAYMENT` header → x402 handles it, MPP middleware is skipped
- No `X-PAYMENT` header → x402 calls `next()`, MPP middleware handles its challenge/response
- Neither payment present → MPP returns its challenge (backward compatible)

### Self-Discovery Endpoint (GET /api)

The `payment` field changes from a string array to structured objects:

```json
{
  "payment": [
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
      "facilitator": "https://facilitator.x402.org"
    }
  ]
}
```

In TESTNET mode, add `"testnet": true` to the top-level response so agents know payments are bypassed.

### TESTNET Behavior

- `x402Charge` → calls `next()` immediately (same pattern as MPP's `noopCharge`)
- MPP → uses existing `noopCharge` (unchanged)
- GET `/api` → lists both payment methods, adds `"testnet": true` flag

## Payment Protocol Detection

Detection is header-based, no client configuration needed:

| Header present | Protocol used |
|---------------|---------------|
| `X-PAYMENT` | x402 |
| MPP headers | MPP/Tempo |
| Neither | MPP challenge response (backward compatible) |

## Files Changed

1. `src/x402.ts` (new) — x402 initialization, `x402Charge` middleware
2. `src/index.ts` (modified) — import x402, wire into routes, update GET /api response
3. `package.json` (modified) — add `@x402/core`, `@x402/evm`
