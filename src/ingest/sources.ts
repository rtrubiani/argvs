export interface DataSource {
  id: string;
  name: string;
  url: string;
  format: "xml" | "csv";
  filename: string;
}

export const sources: DataSource[] = [
  {
    id: "ofac_sdn",
    name: "OFAC SDN",
    url: "https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml",
    format: "xml",
    filename: "ofac_sdn.xml",
  },
  {
    id: "ofac_consolidated",
    name: "OFAC Consolidated (Non-SDN)",
    url: "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_ADVANCED.XML",
    format: "xml",
    filename: "ofac_consolidated.xml",
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
