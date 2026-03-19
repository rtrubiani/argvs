import { screenEntity } from "./match.js";
import { getDb } from "./db.js";

const testCases = [
  { name: "Viktor Bout", note: "exact match, arms dealer (OFAC)" },
  { name: "Viktor But", note: "typo, should still fuzzy match" },
  { name: "Gazprombank", note: "Russian bank (EU/OFAC)" },
  { name: "Kim Jong Un", note: "DPRK leader (OFAC/UN)" },
  { name: "Mario Rossi", note: "common Italian name, should be clear" },
  { name: "Al-Rashid Trust", note: "terrorist financing (UN/OFAC)" },
];

console.log("=== Argvs Match Engine Test ===\n");

for (const tc of testCases) {
  const start = performance.now();
  const result = screenEntity({ name: tc.name });
  const elapsed = (performance.now() - start).toFixed(1);

  console.log(`--- ${tc.name} (${tc.note}) ---`);
  console.log(`  Risk: ${result.risk_level} | ${elapsed}ms | ${result.matches.length} matches`);

  for (const m of result.matches) {
    console.log(
      `  [${m.confidence}%] ${m.name} (${m.source}, ${m.type}) programs=${m.programs.join(",")}${m.aliases_matched.length ? ` aliases=[${m.aliases_matched.join("; ")}]` : ""}`
    );
  }
  console.log();
}

getDb().close();
