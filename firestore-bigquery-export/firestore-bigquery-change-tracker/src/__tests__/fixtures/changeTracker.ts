import {
  ChangeType,
  FirestoreBigQueryEventHistoryTracker,
  FirestoreDocumentChangeEvent,
} from "../..";

export const changeTracker = ({
  datasetId = "",
  tableId = "",
  datasetLocation = null,
  timePartitioning = null,
  timePartitioningField = null,
  timePartitioningFieldType = null,
  timePartitioningFirestoreField = null,
  transformFunction = null,
  clustering = null,
  bqProjectId = null,
}): FirestoreBigQueryEventHistoryTracker => {
  return new FirestoreBigQueryEventHistoryTracker({
    datasetId,
    tableId,
    datasetLocation,
    timePartitioning,
    timePartitioningField,
    timePartitioningFieldType,
    timePartitioningFirestoreField,
    transformFunction,
    clustering,
    bqProjectId,
  });
};

export const changeTrackerEvent = (
  timestamp = "2022-02-13T10:17:43.505Z",
  operation = ChangeType.CREATE,
  documentName = "testing",
  eventId = "testing",
  documentId = "testing",
  data = { end_date: "01/01/2020" }
): FirestoreDocumentChangeEvent => {
  return { timestamp, operation, documentName, eventId, documentId, data };
};
