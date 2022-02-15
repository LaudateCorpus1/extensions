import { BigQuery } from "@google-cloud/bigquery";
import { FirestoreDocumentChangeEvent } from "../..";

import { changeTracker, changeTrackerEvent } from "../fixtures/changeTracker";

import { deleteTable } from "../fixtures/clearTables";
import { defaultQuery } from "../fixtures/queries";

// process.env.PROJECT_ID = "extensions-testing";
process.env.PROJECT_ID = "messaging-test-4395c";

let bq: BigQuery;
let randomID: string;

const { logger } = require("firebase-functions");
const consoleLogSpy = jest.spyOn(logger, "log").mockImplementation();
describe("Using an alternative bigquery project", () => {
  beforeEach(() => {
    randomID = (Math.random() + 1).toString(36).substring(7);
    bq = new BigQuery();
  });

  describe("has a valid alternative project id", () => {
    let datasetId: string;
    let tableId: string;
    let bqProjectId: string;

    beforeEach(async () => {
      datasetId = `dataset_${randomID}`;
      tableId = `table_${randomID}`;
      bqProjectId = "messaging-test-4395c";
    });

    test("successfully uses alternative project name when provided", async () => {
      const event: FirestoreDocumentChangeEvent = changeTrackerEvent();

      await changeTracker({
        datasetId,
        tableId,
        bqProjectId,
      }).record([event]);

      // Run the query as a job
      const [job] = await bq.createQueryJob({
        query: defaultQuery(bqProjectId, datasetId, tableId),
      });

      // Wait for the query to finish
      const [rows] = await job.getQueryResults();
      expect(rows.length).toEqual(1);
    }, 9000);

    afterEach(async () => {
      await deleteTable({
        projectId: bqProjectId,
        datasetId,
        tableId: `${tableId}_raw_changelog`,
      });
    }, 9000);
  });

  describe("does not have valid alternative project id", () => {
    let datasetId: string;
    let tableId: string;

    beforeEach(async () => {
      datasetId = `dataset_${randomID}`;
      tableId = `table_${randomID}`;
    });

    test.skip("defaults to default project name if none provided", async () => {
      const event: FirestoreDocumentChangeEvent = changeTrackerEvent();

      await changeTracker({
        datasetId,
        tableId,
        bqProjectId: null,
      }).record([event]);

      // Run the query as a job
      const [job] = await bq.createQueryJob({
        query: defaultQuery(
          "extensions-testing",
          datasetId,
          `${tableId}_raw_changelog`
        ),
      });

      // Wait for the query to finish
      const [rows] = await job.getQueryResults();
      expect(rows.length).toEqual(1);
    }, 9000);

    afterEach(
      async () =>
        await deleteTable({
          datasetId,
          tableId: `${tableId}_raw_changelog`,
        })
    );
  });

  test.skip("successfully uses default project Id if provided as an alternative Id", async () => {});

  test.skip("Warns the user if an invalid project Id has been provided", async () => {});
});
