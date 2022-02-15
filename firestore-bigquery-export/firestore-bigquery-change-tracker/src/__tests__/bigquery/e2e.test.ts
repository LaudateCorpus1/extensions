import { BigQuery, Dataset, Table } from "@google-cloud/bigquery";
const { logger } = require("firebase-functions");

import {
  RawChangelogSchema,
  RawChangelogViewSchema,
} from "../../bigquery/schema";

import { ChangeType, FirestoreDocumentChangeEvent } from "../..";
import { FirestoreBigQueryEventHistoryTracker } from "../../bigquery";

process.env.PROJECT_ID = "extensions-testing";

const consoleLogSpy = jest.spyOn(logger, "log").mockImplementation();

let bq: BigQuery;
let randomID: string;

describe("Partitioning", () => {
  describe("a non existing dataset and table", () => {
    beforeEach(() => {
      randomID = (Math.random() + 1).toString(36).substring(7);
      bq = new BigQuery();
    });
    test("does not partition without a defined timePartitioning option", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: null,
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
        .table(`${randomID}_raw_changelog`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("does not partition with an unrecognised timePartitioning option", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "UNKNOWN",
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
        .table(`${randomID}_raw_changelog`);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();

      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("successfully partitions a changelog table with a timePartitioning option only", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: null,
          timePartitioningFieldType: null,
          timePartitioningFirestoreField: null,
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeDefined();
    }, 9000);

    test("does not partition latest view table with a timePartitioning option only", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: null,
          timePartitioningFieldType: null,
          timePartitioningFirestoreField: null,
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("does not partition with without a valid timePartitioningField when including timePartitioning, timePartitioningFieldType and timePartitioningFirestoreField", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: null,
          timePartitioningFieldType: "TIMESTAMP",
          timePartitioningFirestoreField: "end_date",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();

      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("does not partition with without a valid timePartitioningFieldType", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: "end_date",
          timePartitioningFieldType: null,
          timePartitioningFirestoreField: "end_date",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();

      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("does not partition with an unknown timePartitioningFieldType", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: "end_date",
          timePartitioningFieldType: "UNKNOWN",
          timePartitioningFirestoreField: "end_date",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();

      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    test("does not partition with an unknown timePartitioningFirestoreField", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: "end_date",
          timePartitioningFieldType: "UNKNOWN",
          timePartitioningFirestoreField: null,
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const raw_changelog_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_table = bq
        .dataset(`dataset_${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();
      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 16000);

    test.skip("Should not partition when timePartitioningFieldType is a DATE type HOUR has been set as timePartitioning field", () => {});

    afterEach(async () => {
      return new Promise((resolve) => {
        const dataset = bq.dataset(`dataset_${randomID}`);
        const table = dataset.table(`${randomID}_raw_changelog`);

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

  describe("a pre existing dataset and table", () => {
    let dataset: Dataset;
    let raw_changelog_table: Table;
    let raw_latest_table: Table;

    beforeEach(async () => {
      randomID = (Math.random() + 1).toString(36).substring(7);
      bq = new BigQuery();

      [dataset] = await bq.dataset(`dataset_${randomID}`).create();

      [raw_changelog_table] = await dataset.createTable(
        `${randomID}_raw_changelog`,
        {}
      );

      [raw_latest_table] = await dataset.createTable(
        `${randomID}_raw_latest`,
        {}
      );
    });

    test.skip("does not update an existing non partitioned table, that has a valid schema with timePartitioning only", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: "",
          timePartitioningFieldType: null,
          timePartitioningFirestoreField: "",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await raw_changelog_table.setMetadata({
        schema: RawChangelogSchema,
      });
      await raw_latest_table.setMetadata({
        schema: RawChangelogViewSchema,
      });

      await generatedTimePartitionField.record([event]);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();
      expect(latestMetaData.timePartitioning).toBeUndefined();

      expect(consoleLogSpy).toBeCalledWith(
        `Cannot partition an exisiting table dataset_${randomID}}`,
        `BigQuery dataset already exists: dataset_${randomID}`,
        `BigQuery table with name ${randomID}_raw_changelog already exists in dataset_${randomID}!`,
        `View with id ${randomID}_raw_latest already exists in dataset dataset_${randomID}.`
      );
    }, 9000);
    test.skip("does not update an existing non partitioned table, that has valid custom partitioning configuration", async () => {
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
          tableId: `${randomID}`,
          datasetLocation: "",
          timePartitioning: "HOUR",
          timePartitioningField: "test",
          timePartitioningFieldType: "test",
          timePartitioningFirestoreField: "test",
          transformFunction: "",
          clustering: [],
          bqProjectId: "extensions-testing",
        }
      );

      await generatedTimePartitionField.record([event]);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();
      const [latestMetaData] = await raw_latest_table.getMetadata();

      expect(changeLogMetaData.timePartitioning).toBeUndefined();
      expect(latestMetaData.timePartitioning).toBeUndefined();
    }, 9000);

    afterEach(async () => {
      return new Promise((resolve) => {
        const dataset = bq.dataset(`dataset_${randomID}`);
        const table = dataset.table(`${randomID}_raw_changelog`);

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
