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

function makeXmlParser(): XMLParser {
  return new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    processEntities: false,
  });
}

// ---------------------------------------------------------------------------
// OFAC Advanced XML (SDN & Consolidated use same schema)
// ---------------------------------------------------------------------------
function parseOfacAdvancedXml(
  filepath: string,
  sourceId: string
): SanctionEntity[] {
  const xml = readFileSync(filepath, "utf-8");
  const parser = makeXmlParser();
  const doc = parser.parse(xml);

  const root = doc?.Sanctions;
  if (!root) return [];

  // Build reference lookup tables
  const partySubTypes = new Map<string, { name: string; partyTypeId: string }>();
  for (const pst of toArray(
    root.ReferenceValueSets?.PartySubTypeValues?.PartySubType
  )) {
    partySubTypes.set(clean(pst["@_ID"]), {
      name: clean(pst["#text"]),
      partyTypeId: clean(pst["@_PartyTypeID"]),
    });
  }

  const partyTypes = new Map<string, string>();
  for (const pt of toArray(
    root.ReferenceValueSets?.PartyTypeValues?.PartyType
  )) {
    partyTypes.set(clean(pt["@_ID"]), clean(pt["#text"]));
  }

  const areaCodeCountries = new Map<string, string>();
  for (const ac of toArray(
    root.ReferenceValueSets?.AreaCodeValues?.AreaCode
  )) {
    areaCodeCountries.set(clean(ac["@_ID"]), clean(ac["@_Description"]));
  }

  // Build location -> country lookup
  const locationCountries = new Map<string, string>();
  for (const loc of toArray(root.Locations?.Location)) {
    const countryId = clean(loc.LocationCountry?.["@_CountryID"]);
    if (countryId && areaCodeCountries.has(countryId)) {
      locationCountries.set(
        clean(loc["@_ID"]),
        areaCodeCountries.get(countryId)!
      );
    }
  }

  // Build profileId -> programs and date_listed from SanctionsEntries
  const profilePrograms = new Map<string, string[]>();
  const profileDates = new Map<string, string>();
  for (const entry of toArray(root.SanctionsEntries?.SanctionsEntry)) {
    const profileId = clean(entry["@_ProfileID"]);
    const programs: string[] = [];
    for (const measure of toArray(entry.SanctionsMeasure)) {
      const comment = clean(measure.Comment);
      if (comment) programs.push(comment);
    }
    profilePrograms.set(profileId, programs);

    const ev = toArray(entry.EntryEvent)[0];
    if (ev) {
      const date = ev.Date;
      if (date) {
        const y = clean(date.Year);
        const m = clean(date.Month);
        const d = clean(date.Day);
        if (y) profileDates.set(profileId, `${y}-${m?.padStart(2, "0")}-${d?.padStart(2, "0")}`);
      }
    }
  }

  // Parse DistinctParty entries
  const parties = toArray(root.DistinctParties?.DistinctParty);
  const results: SanctionEntity[] = [];

  for (const party of parties) {
    const profile = party.Profile;
    if (!profile) continue;

    const profileId = clean(profile["@_ID"]);
    const partySubTypeId = clean(profile["@_PartySubTypeID"]);
    const subType = partySubTypes.get(partySubTypeId);
    const partyTypeName = subType
      ? partyTypes.get(subType.partyTypeId)?.toLowerCase() ?? ""
      : "";

    let type: SanctionEntity["type"] = "unknown";
    if (partyTypeName.includes("individual")) type = "individual";
    else if (partyTypeName.includes("entity")) type = "entity";
    else if (subType?.name.toLowerCase().includes("vessel")) type = "vessel";
    else if (subType?.name.toLowerCase().includes("aircraft"))
      type = "aircraft";

    // Extract names from Identity > Alias > DocumentedName > NamePartValue
    const identity = toArray(profile.Identity)[0];
    if (!identity) continue;

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
    if (!primaryName) continue;

    // Countries from features referencing locations
    const countries: string[] = [];
    for (const feature of toArray(profile.Feature)) {
      for (const fv of toArray(feature.FeatureVersion)) {
        const locId = clean(fv.VersionLocation?.["@_LocationID"]);
        const country = locationCountries.get(locId);
        if (country) countries.push(country);
      }
    }

    results.push({
      id: `${sourceId}:${profileId}`,
      source: sourceId,
      name: primaryName,
      aliases: [...new Set(aliasNames)],
      type,
      programs: profilePrograms.get(profileId) ?? [],
      countries: [...new Set(countries)],
      identifiers: [],
      date_listed: profileDates.get(profileId) ?? "",
      remarks: clean(party.Comment),
    });
  }

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
