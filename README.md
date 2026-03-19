# ARGVS

**Sanctions screening for the agentic economy.**

> The all-seeing guardian. Named after Argus Panoptes, the hundred-eyed giant who never sleeps.

Argvs screens names against international sanctions lists and returns match confidence in under 200ms. Built for AI agents that need to verify counterparties before transacting. Payments via MPP on Tempo blockchain.

## Coverage

- **OFAC SDN** — U.S. Treasury Specially Designated Nationals
- **OFAC Consolidated** — Non-SDN consolidated lists
- **EU Consolidated** — European Union sanctions
- **UN Consolidated** — United Nations Security Council sanctions
- **UK HMT** — His Majesty's Treasury consolidated list
- **31,000+ entities** indexed with daily automatic refresh

## Endpoints

| Path | Method | Price | Description |
|------|--------|-------|-------------|
| `/api/screen` | POST | $0.03 | Screen a single name |
| `/api/batch` | POST | $0.02/entity | Screen up to 100 names |
| `/api/status` | GET | Free | Data freshness and list status |
| `/api` | GET | Free | Service discovery |
| `/.well-known/mcp.json` | GET | Free | MCP tool discovery |

## Quick Start

```bash
# Install and ingest data
npm install
npm run ingest

# Start the server
npm run dev
```

### Docker

```bash
docker build -t argvs .
docker run -p 3000:3000 argvs
```

## Usage

### Free endpoints

```bash
# Service discovery
curl http://localhost:3000/api

# Data status
curl http://localhost:3000/api/status
```

### Paid endpoints (via mppx)

```bash
npm install -g mppx

# Screen a single name
mppx fetch POST http://localhost:3000/api/screen \
  --body '{"name": "Viktor Bout", "type": "individual"}' \
  --method tempo

# Batch screen
mppx fetch POST http://localhost:3000/api/batch \
  --body '{"entities": [{"name": "Kim Jong Un"}, {"name": "Gazprombank"}]}' \
  --method tempo
```

### Example response

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
  "lists_checked": ["ofac_sdn", "ofac_consolidated", "eu", "un", "uk_hmt"],
  "total_entities_screened": 31145,
  "disclaimer": "Automated screening tool. Results should be verified against official sources. Not legal or compliance advice."
}
```

### Risk levels

- **match** (>= 90) — Strong match. Verify against official sources.
- **potential_match** (70-89) — Possible match. Manual review recommended.
- **clear** (< 70) — No significant matches.

## MCP Tool Discovery

Argvs serves `/.well-known/mcp.json` so AI agents (Claude Code, Cursor, etc.) can discover it automatically. See [`mcp/skill.md`](mcp/skill.md) for integration details.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Server port |
| `TEMPO_WALLET_ADDRESS` | (built-in) | Tempo wallet for receiving payments |
| `MPP_SECRET_KEY` | (auto-generated) | HMAC key for payment challenge verification |

## Disclaimer

Argvs is an automated screening tool. Results should be verified against official government sources. This is not legal or compliance advice.

## License

MIT
