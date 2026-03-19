import { createReadStream, readFileSync, statSync } from "node:fs";
import { createInterface } from "node:readline";
import { join, dirname } from "node:path";
import sax from "sax";
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

export function tryGc(): void {
  if (typeof globalThis.gc === "function") {
    globalThis.gc();
  }
}

export function logHeap(label: string): void {
  const used = process.memoryUsage().heapUsed;
  console.log(`[heap] ${label}: ${(used / 1024 / 1024).toFixed(1)} MB`);
}

export function getHeapMB(): number {
  return process.memoryUsage().heapUsed / 1024 / 1024;
}

// ---------------------------------------------------------------------------
// CSV line parser
// ---------------------------------------------------------------------------

export function parseCsvLine(line: string): string[] {
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
// Entity callback type — streaming parsers call this for each entity
// ---------------------------------------------------------------------------

export type EntityCallback = (entity: SanctionEntity) => void;

// ---------------------------------------------------------------------------
// OFAC entity type helper
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
// OFAC SDN CSV — stream line by line
// ---------------------------------------------------------------------------

async function streamOfacSdnCsv(
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  const dataDir = dirname(filepath);

  // Build alias lookup from alt.csv (small file, ~3MB) — streamed
  const aliasMap = new Map<string, string[]>();
  try {
    const altPath = join(dataDir, "ofac_sdn_alt.csv");
    const altStream = createReadStream(altPath, { encoding: "utf-8" });
    const altRl = createInterface({ input: altStream, crlfDelay: Infinity });
    for await (const line of altRl) {
      if (!line.trim()) continue;
      const fields = parseCsvLine(line);
      const entNum = fields[0]?.trim();
      const altName = fields[3]?.trim();
      if (!entNum || !altName || entNum === "-0-") continue;
      if (!aliasMap.has(entNum)) aliasMap.set(entNum, []);
      aliasMap.get(entNum)!.push(altName);
    }
  } catch {
    console.warn("[parse] ofac_sdn: could not read alt.csv, continuing without aliases");
  }

  // Build country lookup from add.csv (small file, ~4MB) — streamed
  const countryMap = new Map<string, string[]>();
  try {
    const addPath = join(dataDir, "ofac_sdn_add.csv");
    const addStream = createReadStream(addPath, { encoding: "utf-8" });
    const addRl = createInterface({ input: addStream, crlfDelay: Infinity });
    for await (const line of addRl) {
      if (!line.trim()) continue;
      const fields = parseCsvLine(line);
      const entNum = fields[0]?.trim();
      const country = fields[4]?.trim();
      if (!entNum || !country || entNum === "-0-") continue;
      if (!countryMap.has(entNum)) countryMap.set(entNum, []);
      countryMap.get(entNum)!.push(country);
    }
  } catch {
    console.warn("[parse] ofac_sdn: could not read add.csv, continuing without countries");
  }

  // Stream main sdn.csv
  const size = statSync(filepath).size;
  console.log(`[parse] ofac_sdn: ${(size / 1024 / 1024).toFixed(1)}MB CSV`);

  let count = 0;
  const stream = createReadStream(filepath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
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

    onEntity({
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
    count++;
  }

  console.log(`[parse] ofac_sdn: ${count} entities parsed`);
  return count;
}

// ---------------------------------------------------------------------------
// OFAC Consolidated CSV — stream line by line
// ---------------------------------------------------------------------------

async function streamOfacConsolidatedCsv(
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  const size = statSync(filepath).size;
  console.log(`[parse] ofac_consolidated: ${(size / 1024 / 1024).toFixed(1)}MB CSV`);

  let count = 0;
  const stream = createReadStream(filepath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
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

    onEntity({
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
    count++;
  }

  console.log(`[parse] ofac_consolidated: ${count} entities parsed`);
  return count;
}

// ---------------------------------------------------------------------------
// UK HMT CSV — stream line by line, group by Group ID
// ---------------------------------------------------------------------------

async function streamUkCsv(
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  const size = statSync(filepath).size;
  console.log(`[parse] uk_hmt: ${(size / 1024 / 1024).toFixed(1)}MB CSV`);

  const stream = createReadStream(filepath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let lineNum = 0;
  let headers: string[] = [];
  const grouped = new Map<string, string[][]>();

  for await (const line of rl) {
    lineNum++;
    if (lineNum === 1) continue; // "Last Updated,<date>" — skip
    if (lineNum === 2) {
      headers = parseCsvLine(line);
      continue;
    }
    if (!line.trim()) continue;
    const row = parseCsvLine(line);
    const groupIdIdx = headers.indexOf("Group ID");
    const groupId = groupIdIdx >= 0 ? (row[groupIdIdx] ?? "").trim() : "";
    if (!groupId) continue;
    if (!grouped.has(groupId)) grouped.set(groupId, []);
    grouped.get(groupId)!.push(row);
  }

  const col = (row: string[], name: string) => {
    const idx = headers.indexOf(name);
    return idx >= 0 ? (row[idx] ?? "").trim() : "";
  };

  let count = 0;
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

    onEntity({
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
    count++;
  }

  // Free the grouped map immediately
  grouped.clear();

  console.log(`[parse] uk_hmt: ${count} entities parsed`);
  return count;
}

// ---------------------------------------------------------------------------
// EU XML — SAX streaming parser
// ---------------------------------------------------------------------------

function clean(val: unknown): string {
  if (val == null) return "";
  return String(val).trim();
}

async function streamEuXml(
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  const size = statSync(filepath).size;
  console.log(`[parse] eu: ${(size / 1024 / 1024).toFixed(1)}MB XML (SAX streaming)`);

  return new Promise((resolve, reject) => {
    const parser = sax.createStream(false, {
      lowercase: true,
      trim: true,
      normalize: true,
    });

    let count = 0;
    const tagStack: string[] = [];

    // Current entity state
    let inEntity = false;
    let entityRefNum = "";
    let entityDesignationDate = "";
    let entityRemark = "";
    let entitySubjectTypeCode = "";

    // nameAlias entries
    let nameAliases: Array<{
      wholeName: string;
      nameStatus: string;
      strong: string;
    }> = [];

    // regulation entries
    let regulations: string[] = [];

    // citizenship entries
    let citizenships: string[] = [];

    // identification entries
    let identifications: Array<{ type: string; number: string }> = [];

    parser.on("opentag", (node: sax.Tag) => {
      const tag = node.name;
      tagStack.push(tag);

      if (tag === "sanctionentity") {
        inEntity = true;
        entityRefNum = (node.attributes.eureferenceNumber as string) ?? "";
        entityDesignationDate = (node.attributes.designationdate as string) ?? "";
        entityRemark = "";
        entitySubjectTypeCode = "";
        nameAliases = [];
        regulations = [];
        citizenships = [];
        identifications = [];
      } else if (inEntity && tag === "namealias") {
        nameAliases.push({
          wholeName: (node.attributes.wholename as string) ?? "",
          nameStatus: (node.attributes.namestatus as string) ?? "",
          strong: (node.attributes.strong as string) ?? "",
        });
      } else if (inEntity && tag === "subjecttype") {
        entitySubjectTypeCode = ((node.attributes.code as string) ?? "").toLowerCase();
      } else if (inEntity && tag === "regulation") {
        const programme = (node.attributes.programme as string) ?? "";
        if (programme) regulations.push(programme);
      } else if (inEntity && tag === "citizenship") {
        const cd = (node.attributes.countrydescription as string) ?? "";
        if (cd) citizenships.push(cd);
      } else if (inEntity && tag === "identification") {
        identifications.push({
          type: (node.attributes.identificationtypedescription as string) ?? "",
          number: (node.attributes.number as string) ?? "",
        });
      }
    });

    parser.on("text", (text: string) => {
      if (inEntity && tagStack[tagStack.length - 1] === "remark") {
        entityRemark += text;
      }
    });

    parser.on("closetag", (tag: string) => {
      tagStack.pop();

      if (tag === "sanctionentity" && inEntity) {
        // Build entity and emit immediately
        const primaryName =
          nameAliases.find(
            (n) => n.nameStatus === "primary" || n.strong === "true"
          ) || nameAliases[0];
        const name = clean(primaryName?.wholeName);

        if (name) {
          const aliases = nameAliases
            .filter((n) => n !== primaryName)
            .map((n) => clean(n.wholeName))
            .filter(Boolean);

          let type: SanctionEntity["type"] = "unknown";
          if (entitySubjectTypeCode.includes("person")) type = "individual";
          else if (entitySubjectTypeCode.includes("enterprise")) type = "entity";

          const identifiers = identifications
            .map((id) => `${clean(id.type)}: ${clean(id.number)}`)
            .filter((s) => s !== ": ");

          onEntity({
            id: `eu:${clean(entityRefNum)}`,
            source: "eu",
            name,
            aliases,
            type,
            programs: [...new Set(regulations)],
            countries: [...new Set(citizenships)],
            identifiers,
            date_listed: clean(entityDesignationDate),
            remarks: clean(entityRemark),
          });
          count++;
        }

        // Reset state — free arrays immediately
        inEntity = false;
        nameAliases = [];
        regulations = [];
        citizenships = [];
        identifications = [];
      }
    });

    parser.on("error", (err: Error) => {
      console.warn(`[parse] eu: SAX error (continuing): ${err.message}`);
      (parser as any)._parser.error = null;
      (parser as any)._parser.resume();
    });

    parser.on("end", () => {
      console.log(`[parse] eu: ${count} entities parsed`);
      resolve(count);
    });

    const fileStream = createReadStream(filepath, { encoding: "utf-8" });
    fileStream.on("error", reject);
    fileStream.pipe(parser);
  });
}

// ---------------------------------------------------------------------------
// UN XML — SAX streaming parser
// ---------------------------------------------------------------------------

async function streamUnXml(
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  const size = statSync(filepath).size;
  console.log(`[parse] un: ${(size / 1024 / 1024).toFixed(1)}MB XML (SAX streaming)`);

  return new Promise((resolve, reject) => {
    const parser = sax.createStream(false, {
      lowercase: true,
      trim: true,
      normalize: true,
    });

    let count = 0;
    const tagStack: string[] = [];
    let textBuffer = "";

    // Track if we're in INDIVIDUAL or ENTITY
    let recordType: "individual" | "entity" | null = null;

    // Current record fields
    let dataId = "";
    let firstName = "";
    let secondName = "";
    let thirdName = "";
    let listedOn = "";
    let comments = "";
    let unListTypes: string[] = [];

    // Alias collection
    let aliases: string[] = [];
    let currentAliasName = "";

    // Address/country collection
    let countries: string[] = [];
    let currentCountry = "";

    // Document collection
    let identifiers: string[] = [];
    let currentDocType = "";
    let currentDocNumber = "";

    function resetRecord() {
      recordType = null;
      dataId = "";
      firstName = "";
      secondName = "";
      thirdName = "";
      listedOn = "";
      comments = "";
      unListTypes = [];
      aliases = [];
      currentAliasName = "";
      countries = [];
      currentCountry = "";
      identifiers = [];
      currentDocType = "";
      currentDocNumber = "";
    }

    parser.on("opentag", (node: sax.Tag) => {
      const tag = node.name;
      tagStack.push(tag);
      textBuffer = "";

      if (tag === "individual") {
        resetRecord();
        recordType = "individual";
      } else if (tag === "entity") {
        resetRecord();
        recordType = "entity";
      }
    });

    parser.on("text", (text: string) => {
      textBuffer += text;
    });

    parser.on("cdata", (text: string) => {
      textBuffer += text;
    });

    parser.on("closetag", (tag: string) => {
      const text = textBuffer.trim();

      if (recordType) {
        if (tag === "dataid") dataId = text;
        else if (tag === "first_name") firstName = text;
        else if (tag === "second_name") secondName = text;
        else if (tag === "third_name") thirdName = text;
        else if (tag === "listed_on") listedOn = text;
        else if (tag === "comments1") comments = text;
        else if (tag === "un_list_type" && text) unListTypes.push(text);
        else if (tag === "alias_name") currentAliasName = text;
        else if (
          (tag === "individual_alias" || tag === "entity_alias") &&
          currentAliasName
        ) {
          aliases.push(currentAliasName);
          currentAliasName = "";
        }
        else if (tag === "country") currentCountry = text;
        else if (
          (tag === "individual_address" || tag === "entity_address") &&
          currentCountry
        ) {
          countries.push(currentCountry);
          currentCountry = "";
        }
        else if (tag === "type_of_document") currentDocType = text;
        else if (tag === "number") currentDocNumber = text;
        else if (tag === "individual_document") {
          if (currentDocType || currentDocNumber) {
            identifiers.push(`${currentDocType}: ${currentDocNumber}`);
          }
          currentDocType = "";
          currentDocNumber = "";
        }
        // Emit on record close
        else if (tag === "individual" || tag === "entity") {
          let name: string;
          if (recordType === "individual") {
            name = [firstName, secondName, thirdName].filter(Boolean).join(" ");
          } else {
            name = firstName;
          }

          if (name && dataId) {
            onEntity({
              id: `un:${dataId}`,
              source: "un",
              name,
              aliases: aliases.filter(Boolean),
              type: recordType,
              programs: unListTypes,
              countries: [...new Set(countries)],
              identifiers,
              date_listed: listedOn,
              remarks: comments,
            });
            count++;
          }

          resetRecord();
        }
      }

      tagStack.pop();
      textBuffer = "";
    });

    parser.on("error", (err: Error) => {
      console.warn(`[parse] un: SAX error (continuing): ${err.message}`);
      (parser as any)._parser.error = null;
      (parser as any)._parser.resume();
    });

    parser.on("end", () => {
      console.log(`[parse] un: ${count} entities parsed`);
      resolve(count);
    });

    const fileStream = createReadStream(filepath, { encoding: "utf-8" });
    fileStream.on("error", reject);
    fileStream.pipe(parser);
  });
}

// ---------------------------------------------------------------------------
// Dispatcher — all parsers are streaming, calling onEntity per record
// ---------------------------------------------------------------------------

export async function streamParseSource(
  source: DataSource,
  filepath: string,
  onEntity: EntityCallback
): Promise<number> {
  switch (source.id) {
    case "ofac_sdn":
      return streamOfacSdnCsv(filepath, onEntity);
    case "ofac_consolidated":
      return streamOfacConsolidatedCsv(filepath, onEntity);
    case "eu":
      return streamEuXml(filepath, onEntity);
    case "un":
      return streamUnXml(filepath, onEntity);
    case "uk_hmt":
      return streamUkCsv(filepath, onEntity);
    default:
      console.warn(`No parser for source: ${source.id}`);
      return 0;
  }
}

// Legacy sync interface (kept for compatibility but avoid using)
export function parseSource(
  source: DataSource,
  filepath: string
): SanctionEntity[] {
  console.warn("[parse] WARNING: using sync parseSource — prefer streamParseSource");
  const entities: SanctionEntity[] = [];
  const raw = readFileSync(filepath, "utf-8");
  if (source.format === "csv") {
    const lines = raw.split("\n");
    for (const line of lines) {
      if (!line.trim()) continue;
      const fields = parseCsvLine(line);
      const entNum = fields[0]?.trim();
      if (!entNum || entNum === "-0-") continue;
      const name = fields[1]?.trim() ?? "";
      if (!name) continue;
      entities.push({
        id: `${source.id}:${entNum}`,
        source: source.id,
        name,
        aliases: [],
        type: "unknown",
        programs: [],
        countries: [],
        identifiers: [],
        date_listed: "",
        remarks: "",
      });
    }
  }
  return entities;
}
