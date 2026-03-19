# Argvs — Sanctions & PEP Screening API

Argvs screens names against international sanctions lists (OFAC SDN, OFAC Consolidated, EU, UN, UK HMT) and Politically Exposed Persons (PEP) data sourced from Wikidata (CC0 license). Results include confidence scores, risk levels, PEP status, and matched list details.

## Quick Start

Base URL: `http://localhost:3000`

Payment: Two options available:
- **MPP/Tempo** — USDC stablecoins on Tempo blockchain
- **x402/Base** — USDC on Base via the x402 payment protocol (facilitator: `https://facilitator.x402.org`)

## Endpoints

### Screen a single name (paid, $0.03)

```bash
curl -X POST http://localhost:3000/api/screen \
  -H "Content-Type: application/json" \
  -d '{"name": "Viktor Bout", "type": "individual"}'
```

### Batch screen (paid, $0.02/entity)

```bash
curl -X POST http://localhost:3000/api/batch \
  -H "Content-Type: application/json" \
  -d '{"entities": [{"name": "Viktor Bout"}, {"name": "Gazprombank"}]}'
```

### Check data status (free)

```bash
curl http://localhost:3000/api/status
```

### Service discovery (free)

```bash
curl http://localhost:3000/api
```

## Payment Methods

### Option 1: MPP/Tempo (mppx CLI)

```bash
# Install mppx
npm install -g mppx

# Screen a name (mppx handles payment automatically)
mppx fetch POST http://localhost:3000/api/screen \
  --body '{"name": "Viktor Bout"}' \
  --method tempo

# Batch screen
mppx fetch POST http://localhost:3000/api/batch \
  --body '{"entities": [{"name": "Kim Jong Un"}, {"name": "Al-Rashid Trust"}]}' \
  --method tempo
```

### Option 2: x402/Base (USDC)

Agents supporting the x402 payment protocol can pay with USDC on Base. The server returns HTTP 402 with payment requirements; the agent signs a USDC transfer authorization and re-sends with an `X-PAYMENT` header. Settlement is handled by the public facilitator at `https://facilitator.x402.org`.

## MCP Tool Discovery

Argvs serves `/.well-known/mcp.json` for automatic tool discovery by AI agents (Claude Code, Cursor, etc.). Point your MCP client at `http://localhost:3000` and it will discover the `screen_sanctions` and `batch_screen_sanctions` tools automatically.

## Response Format

```json
{
  "query": "Viktor Bout",
  "screened_at": "2026-03-19T12:00:00.000Z",
  "matches": [
    {
      "name": "BOUT Viktor Anatolijevitch",
      "source": "ofac_sdn",
      "type": "individual",
      "programs": ["DRCONGO"],
      "countries": [],
      "confidence": 90,
      "aliases_matched": []
    }
  ],
  "risk_level": "match",
  "pep_status": false,
  "lists_checked": ["ofac_sdn", "ofac_consolidated", "eu", "un", "uk_hmt", "PEP"],
  "total_entities_screened": 31145,
  "disclaimer": "Automated screening tool. Results should be verified against official sources. Not legal or compliance advice."
}
```

## PEP Screening

PEP (Politically Exposed Persons) data is sourced from Wikidata's SPARQL endpoint (CC0 license, no commercial restrictions). The database includes:
- Heads of state and government
- Members of parliament/congress/senate
- Cabinet ministers
- Supreme/constitutional court judges
- Central bank governors

PEP matches include a `position` field showing the political office held. The `pep_status` field in the response is `true` when any PEP match is found above 70% confidence. PEP screening is included in the existing pricing — no additional cost.

## Risk Levels

- **match** (confidence >= 90): Strong match found. Verify against official sources.
- **potential_match** (confidence 70-89): Possible match. Manual review recommended. Also set if PEP match found above 70%.
- **clear** (confidence < 70): No significant matches found (and no PEP matches above 70%).
