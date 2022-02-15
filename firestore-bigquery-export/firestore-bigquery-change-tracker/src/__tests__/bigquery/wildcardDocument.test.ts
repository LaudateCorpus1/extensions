import { BigQuery } from "@google-cloud/bigquery";
import {
  ChangeType,
  FirestoreBigQueryEventHistoryTracker,
  FirestoreDocumentChangeEvent,
} from "../..";

process.env.PROJECT_ID = "extensions-testing";

let bq: BigQuery;
let randomID: string;

const { logger } = require("firebase-functions");
const consoleLogSpy = jest.spyOn(logger, "log").mockImplementation();
describe("Configuring a document wildcard column ", () => {
  describe("A non existing table", () => {
    beforeEach(() => {
      randomID = (Math.random() + 1).toString(36).substring(7);
      bq = new BigQuery();
    });
    test.skip("Successfully adds a path_params column with single JSON param when a collection path has a single wildcard.", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        pathParams: { postId: "post1" },
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
          wildcardIds: true,
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
      const raw_changelog_table = await bq
        .dataset(`${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_view = await bq
        .dataset(`${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();

      const isChangelogPathParams = changeLogMetaData.schema.fields.find(
        (column) => column.name === "path_params"
      );

      expect(isChangelogPathParams).toBeTruthy();

      const changelog_rows = await raw_changelog_table.getRows();
      const path_params = JSON.parse(changelog_rows[0][0].path_params);

      expect(path_params.postId).toBe("post1");
    }, 20000);

    test("Successfully adds a path_params column with multiple ids a collection path has more than one wildcard", async () => {
      const event: FirestoreDocumentChangeEvent = {
        timestamp: "2022-02-13T10:17:43.505Z",
        operation: ChangeType.CREATE,
        pathParams: { postId: "post1", userId: "user1" },
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
          wildcardIds: true,
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
      const raw_changelog_table = bq
        .dataset(`${randomID}`)
        .table(`${randomID}_raw_changelog`);

      const raw_latest_view = await bq
        .dataset(`${randomID}`)
        .table(`${randomID}_raw_latest`);

      const [changeLogMetaData] = await raw_changelog_table.getMetadata();

      const [latestViewMetaData] = await raw_latest_view.getMetadata();

      const isChangelogPathParams = changeLogMetaData.schema.fields.find(
        (column) => column.name === "path_params"
      );

      const isViewPathParams = latestViewMetaData.schema.fields.find(
        (column) => column.name === "path_params"
      );

      expect(isChangelogPathParams).toBeTruthy();
      expect(isViewPathParams).toBeTruthy();

      const changelog_rows = await raw_changelog_table.getRows();
      const path_params = JSON.parse(changelog_rows[0][0].path_params);

      expect(path_params.postId).toBe("post1");
      expect(path_params.userId).toBe("user1");
    }, 20000);

    test.skip("Successfully adds a path_params column with an empty JSON object if a collection path has no wildcards", async () => {});

    test.skip("Successfully updates an existing path_params column ", async () => {});

    test.skip("Does not add a path_params column if wilcard config has not been included", async () => {});

    afterEach(async () => {
      return new Promise((resolve) => {
        const dataset = bq.dataset(`${randomID}`);
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

  describe("An already existing table", () => {
    test.skip("Successfully adds a path_params column with a single JSON param when a collection path has a single wildcard.", async () => {});

    test.skip("Successfully adds a path_params column with multiple ids a collection path has more than one wildcard", async () => {});

    test.skip("Successfully adds a path_params column with an empty JSON object if a collection path has no wildcards", async () => {});

    test.skip("Successfully updates an existing path_params column ", async () => {});

    test.skip("Does not add a path_params column if wilcard config has not been included", async () => {});
  });
});
