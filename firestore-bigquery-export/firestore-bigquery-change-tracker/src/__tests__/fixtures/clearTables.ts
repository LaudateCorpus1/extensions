import { BigQuery } from "@google-cloud/bigquery";

export const deleteTable = async ({
  projectId = "extensions-testing",
  datasetId = "",
  tableId = "",
}): Promise<void> => {
  const bq = new BigQuery({ projectId });
  return new Promise((resolve) => {
    const dataset = bq.dataset(datasetId);
    const table = dataset.table(tableId);

    let handle = setInterval(async () => {
      const [datasetExists] = await dataset.exists();
      const [tableExists] = await table.exists();

      if (datasetExists && tableExists) {
        clearInterval(handle);
        return resolve(null);
      }

      try {
        if (datasetExists) {
          await dataset.delete({ force: true });
        }

        if (tableExists) {
          await table.delete();
        }
      } catch (ex) {
        console.warn(
          `Attempted to clear ${dataset.id} and ${table.id}`,
          ex.message
        );
      }
    }, 500);
  });
};
