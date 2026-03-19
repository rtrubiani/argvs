import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { XMLParser } from "fast-xml-parser";
import type { DataSource } from "./sources.js";

export interface SanctionEntity {
  id: string;
  source: string;
  name: string;
  aliases: string[];
  type: "individual" | "entity" | "vessel" | "aircraft" | "unknown";
  programs: string[];
  countries: string[];
  identifiers: string[];
  date_listed: string;
  remarks: string;
}

function toArray<T>(val: T | T[] | undefined | null): T[] {
  if (val == null) return [];
  return Array.isArray(val) ? val : [val];
}

function clean(val: unknown): string {
  if (val == null) return "";
  return String(val).trim();
}

export function tryGc(): void {
  if (typeof globalThis.gc === "function") {
    globalThis.gc();
  }
}

function makeXmlParser(): XMLParser {
  return new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    processEntities: false,
  });
}

// ---------------------------------------------------------------------------
// Shared CSV helper: determine OFAC entity type from SDN_Type field
// ---------------------------------------------------------------------------
function ofacEntityType(sdnType: string): SanctionEntity["type"] {
  const t = sdnType.toLowerCase().trim();
  if (t === "individual") return "individual";
  if (t === "entity") return "entity";
  if (t === "vessel") return "vessel";
  if (t === "aircraft") return "aircraft";
  return "unknown";
}

// ---------------------------------------------------------------------------
// OFAC SDN CSV — line-by-line parsing of sdn.csv + alt.csv + add.csv
//
// sdn.csv columns (no header, positional):
//   0: ent_num, 1: SDN_Name, 2: SDN_Type, 3: Program, 4: Title,
//   5: Call_Sign, 6: Vess_type, 7: Tonnage, 8: GRT, 9: Vess_flag,
//   10: Vess_owner, 11: Remarks
//
// alt.csv columns: ent_num, alt_num, alt_type, alt_name, alt_remarks
// add.csv columns: ent_num, add_num, Address, City, Country, add_remarks
// ---------------------------------------------------------------------------
function parseOfacSdnCsv(filepath: string): SanctionEntity[] {
  const dataDir = dirname(filepath);

  // Build alias lookup from alt.csv (ent_num → alias names)
  const aliasMap = new Map<string, string[]>();
  try {
    const altRaw = readFileSync(join(dataDir, "ofac_sdn_alt.csv"), "utf-8");
    const altLines = altRaw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
    for (const line of altLines) {
      if (!line.trim()) continue;
      const fields = parseCsvLine(line);
      const entNum = fields[0]?.trim();
      const altName = fields[3]?.trim();
      if (!entNum || !altName || entNum === "-0-") continue;
      if (!aliasMap.has(entNum)) aliasMap.set(entNum, []);
      aliasMap.get(entNum)!.push(altName);
    }
  } catch (err) {
    console.warn("[parse] ofac_sdn: could not read alt.csv, continuing without aliases");
  }

  // Build country lookup from add.csv (ent_num → countries)
  const countryMap = new Map<string, string[]>();
  try {
    const addRaw = readFileSync(join(dataDir, "ofac_sdn_add.csv"), "utf-8");
    const addLines = addRaw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
    for (const line of addLines) {
      if (!line.trim()) continue;
      const fields = parseCsvLine(line);
      const entNum = fields[0]?.trim();
      const country = fields[4]?.trim();
      if (!entNum || !country || entNum === "-0-") continue;
      if (!countryMap.has(entNum)) countryMap.set(entNum, []);
      countryMap.get(entNum)!.push(country);
    }
  } catch (err) {
    console.warn("[parse] ofac_sdn: could not read add.csv, continuing without countries");
  }

  // Parse main sdn.csv line by line
  const raw = readFileSync(filepath, "utf-8");
  console.log(
    `[parse] ofac_sdn: ${(raw.length / 1024 / 1024).toFixed(1)}MB CSV`
  );
  const lines = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");

  const results: SanctionEntity[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    const fields = parseCsvLine(line);
    const entNum = fields[0]?.trim();
    if (!entNum || entNum === "-0-") continue;

    const name = fields[1]?.trim() ?? "";
    if (!name) continue;

    const sdnType = fields[2]?.trim() ?? "";
    const program = fields[3]?.trim() ?? "";
    const remarks = fields[11]?.trim() ?? "";

    const programs = program
      ? program.split(";").map((p) => p.trim()).filter(Boolean)
      : [];

    const aliases = aliasMap.get(entNum) ?? [];
    const countries = [...new Set(countryMap.get(entNum) ?? [])];

    results.push({
      id: `ofac_sdn:${entNum}`,
      source: "ofac_sdn",
      name,
      aliases: [...new Set(aliases)],
      type: ofacEntityType(sdnType),
      programs,
      countries,
      identifiers: [],
      date_listed: "",
      remarks: remarks === "-0-" ? "" : remarks,
    });
  }

  console.log(`[parse] ofac_sdn: ${results.length} entities parsed`);
  return results;
}

// ---------------------------------------------------------------------------
// OFAC Consolidated CSV — line-by-line parsing of cons_prim.csv
//
// Same column layout as sdn.csv (no header, positional):
//   0: ent_num, 1: SDN_Name, 2: SDN_Type, 3: Program, 4: Title,
//   5: Call_Sign, 6: Vess_type, 7: Tonnage, 8: GRT, 9: Vess_flag,
//   10: Vess_owner, 11: Remarks
// ---------------------------------------------------------------------------
function parseOfacConsolidatedCsv(filepath: string): SanctionEntity[] {
  const raw = readFileSync(filepath, "utf-8");
  console.log(
    `[parse] ofac_consolidated: ${(raw.length / 1024 / 1024).toFixed(1)}MB CSV`
  );
  const lines = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");

  const results: SanctionEntity[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    const fields = parseCsvLine(line);
    const entNum = fields[0]?.trim();
    if (!entNum || entNum === "-0-") continue;

    const name = fields[1]?.trim() ?? "";
    if (!name) continue;

    const sdnType = fields[2]?.trim() ?? "";
    const program = fields[3]?.trim() ?? "";
    const remarks = fields[11]?.trim() ?? "";

    const programs = program
      ? program.split(";").map((p) => p.trim()).filter(Boolean)
      : [];

    results.push({
      id: `ofac_consolidated:${entNum}`,
      source: "ofac_consolidated",
      name,
      aliases: [],
      type: ofacEntityType(sdnType),
      programs,
      countries: [],
      identifiers: [],
      date_listed: "",
      remarks: remarks === "-0-" ? "" : remarks,
    });
  }

  console.log(`[parse] ofac_consolidated: ${results.length} entities parsed`);
  return results;
}

// ---------------------------------------------------------------------------
// EU Consolidated
// ---------------------------------------------------------------------------
function parseEuXml(filepath: string): SanctionEntity[] {
  const xml = readFileSync(filepath, "utf-8");
  const parser = makeXmlParser();
  const doc = parser.parse(xml);

  const entities = toArray(doc?.export?.sanctionEntity);
  return entities.map((ent: any) => {
    const nameAlias = toArray(ent.nameAlias);
    const primaryName =
      nameAlias.find(
        (n: any) =>
          clean(n["@_nameStatus"]) === "primary" ||
          clean(n["@_strong"]) === "true"
      ) || nameAlias[0];
    const name = clean(primaryName?.["@_wholeName"]);

    const aliases = nameAlias
      .filter((n: any) => n !== primaryName)
      .map((n: any) => clean(n["@_wholeName"]))
      .filter(Boolean);

    const subjectType = clean(ent?.subjectType?.["@_code"]).toLowerCase();
    let type: SanctionEntity["type"] = "unknown";
    if (subjectType.includes("person")) type = "individual";
    else if (subjectType.includes("enterprise")) type = "entity";

    const regulations = toArray(ent.regulation);
    const programs = regulations
      .map((r: any) => clean(r["@_programme"]))
      .filter(Boolean);

    const citizenships = toArray(ent.citizenship);
    const countries = [
      ...new Set(
        citizenships
          .map((c: any) => clean(c["@_countryDescription"]))
          .filter(Boolean)
      ),
    ];

    const identifications = toArray(ent.identification);
    const identifiers = identifications
      .map(
        (id: any) =>
          `${clean(id["@_identificationTypeDescription"])}: ${clean(id["@_number"])}`
      )
      .filter((s: string) => s !== ": ");

    return {
      id: `eu:${clean(ent["@_euReferenceNumber"])}`,
      source: "eu",
      name,
      aliases,
      type,
      programs: [...new Set(programs)],
      countries,
      identifiers,
      date_listed: clean(ent["@_designationDate"]),
      remarks: clean(ent.remark),
    };
  });
}

// ---------------------------------------------------------------------------
// UN Consolidated
// ---------------------------------------------------------------------------
function parseUnXml(filepath: string): SanctionEntity[] {
  const xml = readFileSync(filepath, "utf-8");
  const parser = makeXmlParser();
  const doc = parser.parse(xml);

  const results: SanctionEntity[] = [];
  const root = doc?.CONSOLIDATED_LIST;

  // Individuals
  const individuals = toArray(root?.INDIVIDUALS?.INDIVIDUAL);
  for (const ind of individuals) {
    const parts = [
      clean(ind.FIRST_NAME),
      clean(ind.SECOND_NAME),
      clean(ind.THIRD_NAME),
    ].filter(Boolean);
    const name = parts.join(" ");

    const aliases = toArray(ind.INDIVIDUAL_ALIAS).map((a: any) =>
      [clean(a.ALIAS_NAME)].filter(Boolean).join(" ")
    );

    const countries = [
      ...new Set(
        toArray(ind.INDIVIDUAL_ADDRESS)
          .map((a: any) => clean(a.COUNTRY))
          .filter(Boolean)
      ),
    ];

    const identifiers = toArray(ind.INDIVIDUAL_DOCUMENT).map(
      (d: any) => `${clean(d.TYPE_OF_DOCUMENT)}: ${clean(d.NUMBER)}`
    );

    results.push({
      id: `un:${clean(ind.DATAID)}`,
      source: "un",
      name,
      aliases: aliases.filter(Boolean),
      type: "individual",
      programs: toArray(ind.UN_LIST_TYPE).map(clean).filter(Boolean),
      countries,
      identifiers,
      date_listed: clean(ind.LISTED_ON),
      remarks: clean(ind.COMMENTS1),
    });
  }

  // Entities
  const ents = toArray(root?.ENTITIES?.ENTITY);
  for (const ent of ents) {
    const name = clean(ent.FIRST_NAME);

    const aliases = toArray(ent.ENTITY_ALIAS)
      .map((a: any) => clean(a.ALIAS_NAME))
      .filter(Boolean);

    const countries = [
      ...new Set(
        toArray(ent.ENTITY_ADDRESS)
          .map((a: any) => clean(a.COUNTRY))
          .filter(Boolean)
      ),
    ];

    results.push({
      id: `un:${clean(ent.DATAID)}`,
      source: "un",
      name,
      aliases,
      type: "entity",
      programs: toArray(ent.UN_LIST_TYPE).map(clean).filter(Boolean),
      countries,
      identifiers: [],
      date_listed: clean(ent.LISTED_ON),
      remarks: clean(ent.COMMENTS1),
    });
  }

  return results;
}

// ---------------------------------------------------------------------------
// UK HMT CSV
// ---------------------------------------------------------------------------
function parseUkCsv(filepath: string): SanctionEntity[] {
  const raw = readFileSync(filepath, "utf-8").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = raw.split("\n");
  if (lines.length < 3) return [];

  // First line is "Last Updated,<date>" — skip it. Headers are on line 2.
  const headers = parseCsvLine(lines[1]);
  const col = (row: string[], name: string) => {
    const idx = headers.indexOf(name);
    return idx >= 0 ? (row[idx] ?? "").trim() : "";
  };

  const grouped = new Map<string, string[][]>();
  for (let i = 2; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const row = parseCsvLine(line);
    const groupId = col(row, "Group ID");
    if (!groupId) continue;
    if (!grouped.has(groupId)) grouped.set(groupId, []);
    grouped.get(groupId)!.push(row);
  }

  const results: SanctionEntity[] = [];
  for (const [groupId, rows] of grouped) {
    const first = rows[0];
    const nameParts = [
      col(first, "Name 6"),
      col(first, "Name 1"),
      col(first, "Name 2"),
      col(first, "Name 3"),
      col(first, "Name 4"),
      col(first, "Name 5"),
    ].filter(Boolean);
    const name = nameParts.join(" ");

    const aliases = rows
      .slice(1)
      .map((r) =>
        [
          col(r, "Name 6"),
          col(r, "Name 1"),
          col(r, "Name 2"),
          col(r, "Name 3"),
          col(r, "Name 4"),
          col(r, "Name 5"),
        ]
          .filter(Boolean)
          .join(" ")
      )
      .filter((a) => a && a !== name);

    const groupType = col(first, "Group Type").toLowerCase();
    let type: SanctionEntity["type"] = "unknown";
    if (groupType.includes("individual")) type = "individual";
    else if (groupType.includes("entity") || groupType.includes("ship"))
      type = "entity";

    const country = col(first, "Country");
    const regime = col(first, "Regime");

    results.push({
      id: `uk_hmt:${groupId}`,
      source: "uk_hmt",
      name,
      aliases: [...new Set(aliases)],
      type,
      programs: regime ? [regime] : [],
      countries: country ? [country] : [],
      identifiers: [],
      date_listed: col(first, "Listed On"),
      remarks: col(first, "Other Information"),
    });
  }

  return results;
}

function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------
export function parseSource(
  source: DataSource,
  filepath: string
): SanctionEntity[] {
  switch (source.id) {
    case "ofac_sdn":
      return parseOfacSdnCsv(filepath);
    case "ofac_consolidated":
      return parseOfacConsolidatedCsv(filepath);
    case "eu":
      return parseEuXml(filepath);
    case "un":
      return parseUnXml(filepath);
    case "uk_hmt":
      return parseUkCsv(filepath);
    default:
      console.warn(`No parser for source: ${source.id}`);
      return [];
  }
}
