import { BigQuery, Dataset, Table } from "@google-cloud/bigquery";
import { ChangeType, FirestoreDocumentChangeEvent } from "../..";
import { FirestoreBigQueryEventHistoryTracker } from "../../bigquery";
import { RawChangelogSchema } from "../../bigquery/schema";

process.env.PROJECT_ID = "extensions-testing";

let bq: BigQuery;
let randomID: string;

const { logger } = require("firebase-functions");

const consoleLogSpy = jest.spyOn(logger, "log").mockImplementation();

describe("Clustering ", () => {
  describe("without an existing table", () => {
    beforeEach(() => {
      randomID = (Math.random() + 1).toString(36).substring(7);
      bq = new BigQuery();
    });

    test("does not error when an empty array of options are provided", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        documentName: "testing",
        eventId: "testing",
        documentId: "testing",
        data: { end_date: "01/01/2020" },
      };

      const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
        {
          datasetId: `dataset_${randomID}`,
          tableId: `table_${randomID}`,
          datasetLocation: "",
          timePartitioning: "",
          timePartitioningField: "",
          timePartitioningFieldType: "",
          timePartitioningFirestoreField: "",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`table_${randomID}_raw_changelog`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();

      expect(changeLogMetaData.clustering).toBeUndefined();

      expect(consoleLogSpy).toBeCalledWith(
        `Creating BigQuery table: ${raw_changelog_table.id}`
      );
    }, 9000);

    test("does not error when an null clustering option has been provided", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        documentName: "testing",
        eventId: "testing",
        documentId: "testing",
        data: { end_date: "01/01/2020" },
      };

      const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
        {
          datasetId: `dataset_${randomID}`,
          tableId: `table_${randomID}`,
          datasetLocation: "",
          timePartitioning: "",
          timePartitioningField: "",
          timePartitioningFieldType: "",
          timePartitioningFirestoreField: "",
          transformFunction: "",
          clustering: null,
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`table_${randomID}_raw_changelog`);

      const [exists] = await raw_changelog_table.exists();
      const [metadata] = await raw_changelog_table.getMetadata();

      expect(metadata.clustering).toBeUndefined();
    }, 9000);

    test("successfully adds clustering with an existing schema field", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        documentName: "testing",
        eventId: "testing",
        documentId: "testing",
        data: { end_date: "01/01/2020" },
      };

      const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
        {
          datasetId: `dataset_${randomID}`,
          tableId: `table_${randomID}`,
          datasetLocation: "",
          timePartitioning: "",
          timePartitioningField: "",
          timePartitioningFieldType: "",
          timePartitioningFirestoreField: "",
          transformFunction: "",
          clustering: ["timestamp"],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`table_${randomID}_raw_changelog`);

      const [metadata] = await raw_changelog_table.getMetadata();

      expect(metadata.clustering).toBeDefined();
      expect(metadata.clustering.fields.length).toBe(1);
      expect(metadata.clustering.fields[0]).toBe("timestamp");
    }, 9000);

    //TODO: Check what happens if no schema exists on an already partitioned table without schema
    test("successfully adds clustering on a currently partitioned imported Firestore field", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        documentName: "testing",
        eventId: "testing",
        documentId: "testing",
        data: { endDate: new Date() },
      };

      const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
        {
          datasetId: `dataset_${randomID}`,
          tableId: `table_${randomID}`,
          datasetLocation: "",
          timePartitioning: "DAY",
          timePartitioningField: "end_date",
          timePartitioningFieldType: "TIMESTAMP",
          timePartitioningFirestoreField: "endDate",
          transformFunction: "",
          clustering: ["end_date"],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`table_${randomID}_raw_changelog`);

      const [metadata] = await raw_changelog_table.getMetadata();

      expect(metadata.clustering).toBeDefined();
      expect(metadata.clustering.fields.length).toBe(1);
      expect(metadata.clustering.fields[0]).toBe("end_date");
    }, 9000);

    afterEach(async () => {
      return new Promise((resolve) => {
        const dataset = bq.dataset(`dataset_${randomID}`);
        const table = dataset.table(`table_${randomID}_raw_changelog`);

        let handle = setInterval(async () => {
          const [datasetExists] = await dataset.exists();
          const [tableExists] = await table.exists();

          if (datasetExists && tableExists) {
            clearInterval(handle);
            return resolve(table);
          }

          try {
            if (datasetExists) {
              await dataset.delete();
            }

            if (tableExists) {
              await table.delete();
            }
          } catch (ex) {
            console.warn(`Attempted to clear ${randomID}`, ex.message);
          }
        }, 500);
      });
    });
  });
});

describe("with existing table", () => {
  let datasetId: string;
  let tableId: string;
  let myTable: Table;
  let myDataset: Dataset;

  beforeEach(async () => {
    randomID = (Math.random() + 1).toString(36).substring(7);
    bq = new BigQuery();

    datasetId = `${randomID}`;
    tableId = `${randomID}_raw_changelog`;

    [myDataset] = await bq.dataset(datasetId).create();
  });

  //TODO: Consider what happens to a partitioned able that was created without a schema, can we add clustering?
  test.skip("successfully adds clustering with a table that has already been partitioned without schema", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      timePartitioning: { type: "HOUR" },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: datasetId,
        tableId: tableId,
        datasetLocation: "",
        timePartitioning: "HOUR",
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["timestamp"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    //Assert
    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
  }, 9000);

  test("successfully adds clustering with a table that has already been partitioned with schema", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      timePartitioning: { type: "HOUR" },
      schema: RawChangelogSchema,
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: "HOUR",
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["timestamp"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);
    //Assert

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
  }, 9000);

  test("delete clustering from partitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      timePartitioning: { type: "HOUR" },
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "data"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: "HOUR",
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: undefined,
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeUndefined();
  }, 9000);

  test("update clustering with different fields for partitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      timePartitioning: { type: "HOUR" },
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "document_id"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: "HOUR",
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["data"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
    expect(metadata.clustering.fields.length).toBe(1);
    expect(metadata.clustering.fields[0]).toBe("data");
  }, 9000);

  test("update clustering with the same values and length but different order for partitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      timePartitioning: { type: "HOUR" },
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "document_id"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: "HOUR",
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["document_id", "timestamp"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
    expect(metadata.clustering.fields.length).toBe(2);
    expect(metadata.clustering.fields[0]).toBe("document_id");
  }, 9000);

  test("update clustering with different values for unpartitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "document_id"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: null,
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["data"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
    expect(metadata.clustering.fields.length).toBe(1);
    expect(metadata.clustering.fields[0]).toBe("data");
  }, 9000);

  test("update clustering with different values for unpartitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "document_id"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: null,
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: ["data"],
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeDefined();
    expect(metadata.clustering.fields.length).toBe(1);
    expect(metadata.clustering.fields[0]).toBe("data");
  }, 9000);

  test("delete clustering with different values for unpartitioned table", async () => {
    [myTable] = await myDataset.createTable(tableId, {
      schema: RawChangelogSchema,
      clustering: { fields: ["timestamp", "document_id"] },
    });
    const event: FirestoreDocumentChangeEvent = {
      timestamp: "2022-02-13T10:17:43.505Z",
      operation: ChangeType.CREATE,
      documentName: "testing",
      eventId: "testing",
      documentId: "testing",
      data: { end_date: "01/01/2020" },
    };

    const generatedTimePartitionField = new FirestoreBigQueryEventHistoryTracker(
      {
        datasetId: randomID,
        tableId: randomID,
        datasetLocation: "",
        timePartitioning: null,
        timePartitioningField: undefined,
        timePartitioningFieldType: undefined,
        timePartitioningFirestoreField: undefined,
        transformFunction: "",
        clustering: undefined,
        bqProjectId: "extensions-testing",
      }
    );
    await generatedTimePartitionField.record([event]);

    const [metadata] = await myTable.getMetadata();

    expect(metadata.clustering).toBeUndefined();
  }, 9000);
  test.skip("successfully adds clustering on a non default schema field", () => {});
  test.skip("successfully adds clustering with a custom field", () => {});

  afterEach(async () => {
    return new Promise((resolve) => {
      const dataset = bq.dataset(datasetId);
      const table = dataset.table(tableId);

      let handle = setInterval(async () => {
        const [datasetExists] = await dataset.exists();
        const [tableExists] = await table.exists();

        if (datasetExists && tableExists) {
          clearInterval(handle);
          return resolve(table);
        }

        try {
          if (datasetExists) {
            await dataset.delete();
          }

          if (tableExists) {
            await table.delete();
          }
        } catch (ex) {
          console.warn(`Attempted to clear ${randomID}`, ex.message);
        }
      }, 500);
    });
  }, 16000);
});
