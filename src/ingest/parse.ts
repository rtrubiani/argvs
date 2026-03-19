import { readFileSync } from "node:fs";
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
// OFAC lookup context (shared across batches)
// ---------------------------------------------------------------------------
interface OfacLookups {
  partySubTypes: Map<string, { name: string; partyTypeId: string }>;
  partyTypes: Map<string, string>;
  locationCountries: Map<string, string>;
  profilePrograms: Map<string, string[]>;
  profileDates: Map<string, string>;
}

// ---------------------------------------------------------------------------
// Process a single parsed DistinctParty object into a SanctionEntity
// ---------------------------------------------------------------------------
function processOfacParty(
  party: any,
  sourceId: string,
  ctx: OfacLookups
): SanctionEntity | null {
  const profile = party.Profile;
  if (!profile) return null;

  const profileId = clean(profile["@_ID"]);
  const partySubTypeId = clean(profile["@_PartySubTypeID"]);
  const subType = ctx.partySubTypes.get(partySubTypeId);
  const partyTypeName = subType
    ? ctx.partyTypes.get(subType.partyTypeId)?.toLowerCase() ?? ""
    : "";

  let type: SanctionEntity["type"] = "unknown";
  if (partyTypeName.includes("individual")) type = "individual";
  else if (partyTypeName.includes("entity")) type = "entity";
  else if (subType?.name.toLowerCase().includes("vessel")) type = "vessel";
  else if (subType?.name.toLowerCase().includes("aircraft")) type = "aircraft";

  const identity = toArray(profile.Identity)[0];
  if (!identity) return null;

  const aliases = toArray(identity.Alias);
  let primaryName = "";
  const aliasNames: string[] = [];

  for (const alias of aliases) {
    const isPrimary = clean(alias["@_Primary"]) === "true";
    const docNames = toArray(alias.DocumentedName);
    for (const docName of docNames) {
      const parts = toArray(docName.DocumentedNamePart);
      const nameParts = parts.map((p: any) =>
        clean(
          Array.isArray(p.NamePartValue)
            ? p.NamePartValue[0]?.["#text"] ?? p.NamePartValue[0]
            : p.NamePartValue?.["#text"] ?? p.NamePartValue
        )
      );
      const fullName = nameParts.filter(Boolean).join(" ");
      if (!fullName) continue;

      if (isPrimary && !primaryName) {
        primaryName = fullName;
      } else {
        aliasNames.push(fullName);
      }
    }
  }

  if (!primaryName) {
    primaryName = aliasNames.shift() ?? "";
  }
  if (!primaryName) return null;

  const countries: string[] = [];
  for (const feature of toArray(profile.Feature)) {
    for (const fv of toArray(feature.FeatureVersion)) {
      const locId = clean(fv.VersionLocation?.["@_LocationID"]);
      const country = ctx.locationCountries.get(locId);
      if (country) countries.push(country);
    }
  }

  return {
    id: `${sourceId}:${profileId}`,
    source: sourceId,
    name: primaryName,
    aliases: [...new Set(aliasNames)],
    type,
    programs: ctx.profilePrograms.get(profileId) ?? [],
    countries: [...new Set(countries)],
    identifiers: [],
    date_listed: ctx.profileDates.get(profileId) ?? "",
    remarks: clean(party.Comment),
  };
}

// ---------------------------------------------------------------------------
// Parse a batch of DistinctParty XML strings
// ---------------------------------------------------------------------------
function processOfacBatch(
  batch: string[],
  parser: XMLParser,
  sourceId: string,
  ctx: OfacLookups,
  results: SanctionEntity[]
): void {
  const batchXml = `<B>${batch.join("")}</B>`;
  const doc = parser.parse(batchXml);
  const parties = toArray(doc?.B?.DistinctParty);

  for (const party of parties) {
    const entity = processOfacParty(party, sourceId, ctx);
    if (entity) results.push(entity);
  }
}

// ---------------------------------------------------------------------------
// OFAC Advanced XML — chunked parsing to stay within 512MB RAM
//
// Instead of parsing the entire 117MB XML at once (which creates a ~400MB JS
// object), we:
//   1. Extract and parse only the reference sections (small)
//   2. Split DistinctParty entries and parse in batches of 200
//   3. Free intermediate data between steps
// ---------------------------------------------------------------------------
const OFAC_BATCH_SIZE = 200;

function parseOfacAdvancedXml(
  filepath: string,
  sourceId: string
): SanctionEntity[] {
  // Step 1: Read full file
  let raw: string | null = readFileSync(filepath, "utf-8");
  console.log(
    `[parse] ${sourceId}: ${(raw.length / 1024 / 1024).toFixed(1)}MB XML`
  );

  // Step 2: Locate DistinctParties section
  const dpStartIdx = raw.indexOf("<DistinctParties");
  const dpCloseTag = "</DistinctParties>";
  const dpEndIdx = raw.lastIndexOf(dpCloseTag);

  if (dpStartIdx === -1 || dpEndIdx === -1) {
    console.warn(`[parse] ${sourceId}: no DistinctParties section found`);
    return [];
  }

  // Find end of the opening tag (handles attributes on the element)
  const dpContentStart = raw.indexOf(">", dpStartIdx) + 1;

  // Step 3: Extract reference XML (everything before DistinctParties)
  // and party content separately, then free the full raw string
  const refXml = raw.substring(0, dpStartIdx) + "</Sanctions>";
  let dpContent: string | null = raw.substring(dpContentStart, dpEndIdx);
  raw = null;
  tryGc();

  // Step 4: Parse reference data and build lookup tables
  const parser = makeXmlParser();
  let refDoc: any = parser.parse(refXml);
  const root = refDoc?.Sanctions;
  if (!root) return [];

  const ctx: OfacLookups = {
    partySubTypes: new Map(),
    partyTypes: new Map(),
    locationCountries: new Map(),
    profilePrograms: new Map(),
    profileDates: new Map(),
  };

  for (const pst of toArray(
    root.ReferenceValueSets?.PartySubTypeValues?.PartySubType
  )) {
    ctx.partySubTypes.set(clean(pst["@_ID"]), {
      name: clean(pst["#text"]),
      partyTypeId: clean(pst["@_PartyTypeID"]),
    });
  }

  for (const pt of toArray(
    root.ReferenceValueSets?.PartyTypeValues?.PartyType
  )) {
    ctx.partyTypes.set(clean(pt["@_ID"]), clean(pt["#text"]));
  }

  const areaCodeCountries = new Map<string, string>();
  for (const ac of toArray(
    root.ReferenceValueSets?.AreaCodeValues?.AreaCode
  )) {
    areaCodeCountries.set(clean(ac["@_ID"]), clean(ac["@_Description"]));
  }

  for (const loc of toArray(root.Locations?.Location)) {
    const countryId = clean(loc.LocationCountry?.["@_CountryID"]);
    if (countryId && areaCodeCountries.has(countryId)) {
      ctx.locationCountries.set(
        clean(loc["@_ID"]),
        areaCodeCountries.get(countryId)!
      );
    }
  }

  for (const entry of toArray(root.SanctionsEntries?.SanctionsEntry)) {
    const profileId = clean(entry["@_ProfileID"]);
    const programs: string[] = [];
    for (const measure of toArray(entry.SanctionsMeasure)) {
      const comment = clean(measure.Comment);
      if (comment) programs.push(comment);
    }
    ctx.profilePrograms.set(profileId, programs);

    const ev = toArray(entry.EntryEvent)[0];
    if (ev) {
      const date = ev.Date;
      if (date) {
        const y = clean(date.Year);
        const m = clean(date.Month);
        const d = clean(date.Day);
        if (y)
          ctx.profileDates.set(
            profileId,
            `${y}-${m?.padStart(2, "0")}-${d?.padStart(2, "0")}`
          );
      }
    }
  }

  // Free parsed reference doc
  refDoc = null;
  tryGc();

  // Step 5: Process DistinctParty entries in batches
  const results: SanctionEntity[] = [];
  const PARTY_OPEN = "<DistinctParty";
  const PARTY_CLOSE = "</DistinctParty>";
  let searchFrom = 0;
  let batch: string[] = [];

  while (true) {
    const start = dpContent!.indexOf(PARTY_OPEN, searchFrom);
    if (start === -1) break;
    const closeIdx = dpContent!.indexOf(PARTY_CLOSE, start);
    if (closeIdx === -1) break;
    const end = closeIdx + PARTY_CLOSE.length;

    batch.push(dpContent!.substring(start, end));
    searchFrom = end;

    if (batch.length >= OFAC_BATCH_SIZE) {
      processOfacBatch(batch, parser, sourceId, ctx, results);
      batch = [];
    }
  }

  if (batch.length > 0) {
    processOfacBatch(batch, parser, sourceId, ctx, results);
  }

  // Free party content
  dpContent = null;
  tryGc();

  console.log(`[parse] ${sourceId}: ${results.length} entities parsed`);
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
      return parseOfacAdvancedXml(filepath, "ofac_sdn");
    case "ofac_consolidated":
      return parseOfacAdvancedXml(filepath, "ofac_consolidated");
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
