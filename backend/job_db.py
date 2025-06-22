from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceModifiedError
from azure.core import MatchConditions
import os

TABLE_NAME = "jobstatus"

def get_table_client():
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    table_service = TableServiceClient.from_connection_string(conn_str)
    return table_service.get_table_client(table_name=TABLE_NAME)


def create_job_entry(job_id: str):
    entity = {
        "PartitionKey": job_id,
        "RowKey": "status",
        "AudioExtracted": False,
        "ChunkUploaded": 0,
        "ChunkWatermarkBusy": 0,
        "ChunkWatermarkDone": 0,
        "TotalNumChunks" : 0,
        "Concat": False,
        "AudioAdded": False,
        "ThumbnailBusy": 0,
        "ThumbnailDone": 0,
        "ThumbnailConcat": False,
    }
    get_table_client().create_entity(entity)


def update_job(job_id: str, updates: dict):
    table_client = get_table_client()
    entity = table_client.get_entity(partition_key=job_id, row_key="status")
    for key, value in updates.items():
        entity[key] = value
    table_client.update_entity(entity=entity, mode=UpdateMode.MERGE)

def atomic_increment(job_id: str, key: str):
    table_client = get_table_client()
    while True:
        entity = table_client.get_entity(partition_key=job_id, row_key="status")
        etag = entity.metadata['etag']

        # Increase the value
        entity[key] += 1

        # Attempt conditional update
        try:
            table_client.update_entity(entity, etag=etag, match_condition=MatchConditions.IfNotModified)
            return entity[key]
        except ResourceModifiedError:
            continue # try again


def get_job(job_id: str):
    return get_table_client().get_entity(partition_key=job_id, row_key="status")
