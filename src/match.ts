import { searchEntities, getDb, type SearchResult } from "./db.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export interface ScreenQuery {
  name: string;
  type?: string;
  country?: string;
}

export interface MatchResult {
  name: string;
  source: string;
  type: string;
  programs: string[];
  countries: string[];
  confidence: number;
  aliases_matched: string[];
}

export interface ScreenResponse {
  query: string;
  screened_at: string;
  matches: MatchResult[];
  risk_level: "clear" | "potential_match" | "match";
  lists_checked: string[];
  total_entities_screened: number;
  disclaimer: string;
}

const DISCLAIMER =
  "Automated screening tool. Results should be verified against official sources. Not legal or compliance advice.";

const LISTS_CHECKED = [
  "ofac_sdn",
  "ofac_consolidated",
  "eu",
  "un",
  "uk_hmt",
];

// ---------------------------------------------------------------------------
// Text helpers
// ---------------------------------------------------------------------------
function normalize(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenize(s: string): string[] {
  return normalize(s).split(" ").filter(Boolean);
}

// ---------------------------------------------------------------------------
// Levenshtein distance (Wagner-Fischer, pure TS)
// ---------------------------------------------------------------------------
function levenshtein(a: string, b: string): number {
  const m = a.length;
  const n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;

  let prev = new Uint16Array(n + 1);
  let curr = new Uint16Array(n + 1);

  for (let j = 0; j <= n; j++) prev[j] = j;

  for (let i = 1; i <= m; i++) {
    curr[0] = i;
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
    }
    [prev, curr] = [curr, prev];
  }

  return prev[n];
}

// ---------------------------------------------------------------------------
// Jaccard similarity on token sets
// ---------------------------------------------------------------------------
function jaccard(a: string[], b: string[]): number {
  if (a.length === 0 && b.length === 0) return 1;
  const setA = new Set(a);
  const setB = new Set(b);
  let intersection = 0;
  for (const t of setA) {
    if (setB.has(t)) intersection++;
  }
  const union = setA.size + setB.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

// ---------------------------------------------------------------------------
// Build FTS5 query from tokens
// ---------------------------------------------------------------------------
function buildFtsQuery(tokens: string[]): string {
  if (tokens.length === 0) return "";
  // Use OR to broaden matches, with prefix matching via *
  return tokens.map((t) => `"${t}"*`).join(" OR ");
}

// ---------------------------------------------------------------------------
// Compute name similarity (order-insensitive, handles extra tokens)
// ---------------------------------------------------------------------------
function nameSimilarity(queryNorm: string, queryTokens: string[], candNorm: string, candTokens: string[]): number {
  // 1. Direct Levenshtein on full strings
  const maxLen = Math.max(queryNorm.length, candNorm.length);
  const directLev = maxLen === 0 ? 1 : 1 - levenshtein(queryNorm, candNorm) / maxLen;

  // 2. Order-insensitive: sort tokens then compare
  const qSorted = [...queryTokens].sort().join(" ");
  const cSorted = [...candTokens].sort().join(" ");
  const sortedMax = Math.max(qSorted.length, cSorted.length);
  const sortedLev = sortedMax === 0 ? 1 : 1 - levenshtein(qSorted, cSorted) / sortedMax;

  // 3. Subset match: if all query tokens are contained in candidate tokens
  // (handles "Viktor Bout" matching "BOUT Viktor Anatolijevitch")
  let subsetSim = 0;
  if (queryTokens.length > 0 && queryTokens.length <= candTokens.length) {
    let matched = 0;
    const used = new Set<number>();
    for (const qt of queryTokens) {
      let bestDist = Infinity;
      let bestIdx = -1;
      for (let i = 0; i < candTokens.length; i++) {
        if (used.has(i)) continue;
        const d = levenshtein(qt, candTokens[i]);
        if (d < bestDist) {
          bestDist = d;
          bestIdx = i;
        }
      }
      if (bestIdx >= 0) {
        const tokenMax = Math.max(qt.length, candTokens[bestIdx].length);
        const tokenSim = tokenMax === 0 ? 1 : 1 - bestDist / tokenMax;
        if (tokenSim >= 0.6) {
          matched++;
          used.add(bestIdx);
        }
      }
    }
    subsetSim = matched / queryTokens.length;
  }

  // 4. Jaccard token overlap
  const jac = jaccard(queryTokens, candTokens);

  // Take the best signal
  return Math.max(directLev, sortedLev, subsetSim, jac);
}

// ---------------------------------------------------------------------------
// Score a candidate against the query
// ---------------------------------------------------------------------------
function scoreCandidate(
  query: ScreenQuery,
  queryNorm: string,
  queryTokens: string[],
  candidate: SearchResult
): { confidence: number; aliases_matched: string[] } {
  const candNorm = normalize(candidate.name);
  const candTokens = tokenize(candidate.name);

  // Primary name similarity
  const primarySim = nameSimilarity(queryNorm, queryTokens, candNorm, candTokens);

  // Check aliases — find best alias match
  let bestAliasSim = 0;
  const aliasesMatched: string[] = [];

  for (const alias of candidate.aliases) {
    const aliasNorm = normalize(alias);
    const aliasTokens = tokenize(alias);
    const aliasSim = nameSimilarity(queryNorm, queryTokens, aliasNorm, aliasTokens);

    if (aliasSim > bestAliasSim) {
      bestAliasSim = aliasSim;
    }
    if (aliasSim >= 0.5) {
      aliasesMatched.push(alias);
    }
  }

  // Take best of primary name vs best alias
  const bestSim = Math.max(primarySim, bestAliasSim);

  // Base score: 0-90 from name similarity
  let score = bestSim * 90;

  // Country boost: +5
  if (query.country) {
    const qCountry = normalize(query.country);
    const matches = candidate.countries.some(
      (c) => normalize(c) === qCountry || normalize(c).includes(qCountry)
    );
    if (matches) score += 5;
  }

  // Type boost: +5
  if (query.type) {
    const qType = normalize(query.type);
    if (normalize(candidate.type) === qType) score += 5;
  }

  // Clamp to 0-100
  const confidence = Math.round(Math.min(100, Math.max(0, score)));

  return { confidence, aliases_matched: aliasesMatched };
}

// ---------------------------------------------------------------------------
// Main screening function
// ---------------------------------------------------------------------------
export function screenEntity(query: ScreenQuery): ScreenResponse {
  const queryNorm = normalize(query.name);
  const queryTokens = tokenize(query.name);
  const ftsQuery = buildFtsQuery(queryTokens);

  const totalEntities = getDb()
    .prepare("SELECT COUNT(*) as count FROM entities")
    .get() as { count: number };

  if (!ftsQuery) {
    return {
      query: query.name,
      screened_at: new Date().toISOString(),
      matches: [],
      risk_level: "clear",
      lists_checked: LISTS_CHECKED,
      total_entities_screened: totalEntities.count,
      disclaimer: DISCLAIMER,
    };
  }

  // Get initial candidates via FTS5 (top 30 by BM25)
  let candidates: SearchResult[];
  try {
    candidates = searchEntities(ftsQuery, 30);
  } catch {
    // FTS5 query syntax can fail on unusual inputs — fall back empty
    candidates = [];
  }

  // Score and filter
  const scored = candidates
    .map((c) => {
      const { confidence, aliases_matched } = scoreCandidate(
        query,
        queryNorm,
        queryTokens,
        c
      );
      return {
        name: c.name,
        source: c.source,
        type: c.type,
        programs: c.programs,
        countries: c.countries,
        confidence,
        aliases_matched,
      };
    })
    .filter((m) => m.confidence >= 60)
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, 5);

  // Determine risk level
  let risk_level: ScreenResponse["risk_level"] = "clear";
  if (scored.length > 0) {
    const topScore = scored[0].confidence;
    if (topScore >= 90) risk_level = "match";
    else if (topScore >= 70) risk_level = "potential_match";
  }

  return {
    query: query.name,
    screened_at: new Date().toISOString(),
    matches: scored,
    risk_level,
    lists_checked: LISTS_CHECKED,
    total_entities_screened: totalEntities.count,
    disclaimer: DISCLAIMER,
  };
}

// ---------------------------------------------------------------------------
// Batch screening
// ---------------------------------------------------------------------------
export function batchScreen(entities: ScreenQuery[]): ScreenResponse[] {
  return entities.map((e) => screenEntity(e));
}
