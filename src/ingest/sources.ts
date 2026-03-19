export interface DataSourceFile {
  url: string;
  filename: string;
}

export interface DataSource {
  id: string;
  name: string;
  url: string;
  format: "xml" | "csv";
  filename: string;
  extraFiles?: DataSourceFile[];
}

export const sources: DataSource[] = [
  {
    id: "ofac_sdn",
    name: "OFAC SDN",
    url: "https://www.treasury.gov/ofac/downloads/sdn.csv",
    format: "csv",
    filename: "ofac_sdn.csv",
    extraFiles: [
      {
        url: "https://www.treasury.gov/ofac/downloads/add.csv",
        filename: "ofac_sdn_add.csv",
      },
      {
        url: "https://www.treasury.gov/ofac/downloads/alt.csv",
        filename: "ofac_sdn_alt.csv",
      },
    ],
  },
  {
    id: "ofac_consolidated",
    name: "OFAC Consolidated (Non-SDN)",
    url: "https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv",
    format: "csv",
    filename: "ofac_consolidated.csv",
  },
  {
    id: "eu",
    name: "EU Consolidated",
    url: "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
    format: "xml",
    filename: "eu_consolidated.xml",
  },
  {
    id: "un",
    name: "UN Consolidated",
    url: "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
    format: "xml",
    filename: "un_consolidated.xml",
  },
  {
    id: "uk_hmt",
    name: "UK HMT",
    url: "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv",
    format: "csv",
    filename: "uk_hmt.csv",
  },
];
