// ---------------------------------------------------------------------------
// Matching engine scoring weights and confidence thresholds.
//
// Loaded at runtime from environment variables. Every parameter has a sensible
// default so the engine works out-of-the-box, but production deployments
// should supply tuned values via env vars or a private config.
// ---------------------------------------------------------------------------

function num(envKey: string, fallback: number): number {
  const v = process.env[envKey];
  if (v === undefined) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

export interface MatchConfig {
  /** Max score contribution from name similarity (0–100 scale). */
  nameWeight: number;
  /** Bonus added when the query country matches a candidate country. */
  countryBoost: number;
  /** Bonus added when the query entity type matches the candidate type. */
  typeBoost: number;
  /** Per-token Levenshtein similarity floor for subset matching (0–1). */
  tokenSimThreshold: number;
  /** Minimum alias similarity to count as a matched alias (0–1). */
  aliasMatchThreshold: number;
  /** Minimum overall confidence to include a result (0–100). */
  minConfidence: number;
  /** Top-score threshold to classify risk as "match" (0–100). */
  matchThreshold: number;
  /** Top-score threshold to classify risk as "potential_match" (0–100). */
  potentialMatchThreshold: number;
  /** Maximum number of results returned per query. */
  maxResults: number;
  /** Number of FTS5 candidates to retrieve before re-scoring. */
  ftsCandidateLimit: number;
}

export const config: MatchConfig = {
  nameWeight:              num("MATCH_NAME_WEIGHT", 90),
  countryBoost:            num("MATCH_COUNTRY_BOOST", 5),
  typeBoost:               num("MATCH_TYPE_BOOST", 5),
  tokenSimThreshold:       num("MATCH_TOKEN_SIM_THRESHOLD", 0.6),
  aliasMatchThreshold:     num("MATCH_ALIAS_MATCH_THRESHOLD", 0.5),
  minConfidence:           num("MATCH_MIN_CONFIDENCE", 60),
  matchThreshold:          num("MATCH_MATCH_THRESHOLD", 90),
  potentialMatchThreshold: num("MATCH_POTENTIAL_MATCH_THRESHOLD", 70),
  maxResults:              num("MATCH_MAX_RESULTS", 5),
  ftsCandidateLimit:       num("MATCH_FTS_CANDIDATE_LIMIT", 30),
};
