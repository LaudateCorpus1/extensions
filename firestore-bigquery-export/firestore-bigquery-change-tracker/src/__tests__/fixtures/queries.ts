export const defaultQuery = (
  bqProjectId: string,
  datasetId: string,
  tableId: string
): string => `SELECT document_id
      FROM \`${bqProjectId}.${datasetId}.${tableId}_raw_changelog\`
      LIMIT 1`;
