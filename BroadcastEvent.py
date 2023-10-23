from databricks.sdk.runtime import *
import uuid
import json
from datetime import datetime, timezone
import asyncio

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from azure.eventhub import EventData

### These values are needed to publish events
CONNECTION_STR = dbutils.secrets.get(scope="SDE-KV", key="broadcastEventEndpoint")
CLIENT_ID = dbutils.secrets.get(scope="SDE-KV", key="eventHubPublishClientId")
EVENT_HUB_NAME = dbutils.secrets.get(scope="SDE-KV", key="eventHubPublishName") 

###
### Quality Completed Event (Producer + Success)
###
async def quality_completed_event(producer, correlationId, engagementId, jobRunId, userId, domain, publishId):
    payloadQualityCompleted = json.dumps({
    "id": str(uuid.uuid4()),
    "topic": "TaxDataExchangeServicesDevUS",
    "subject": "Data Service Completed",
    "data":{
        "data": {
        "jobRunId": jobRunId,
        "userId": userId,
        "publishId": publishId,
        "status": "Success"
        },
        "metadata": {
        "correlationId": correlationId,
        "engagementId": engagementId,
        "domain": domain,
        "service": "SDE Data Service"
        }
    },
    "eventType": "SDE.QualityCompleted",
    "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "metadataVersion": "1",
    "dataVersion": "1"
    })

    event_data_batch = await producer.create_batch()
    event_data_quality_completed = EventData(payloadQualityCompleted)
    event_data_quality_completed.properties = {'ClientId': CLIENT_ID}
    event_data_batch.add(event_data_quality_completed)

    await producer.send_batch(event_data_batch)

async def broadcastQualityCompleted(correlationId, engagementId, jobRunId, userId, domain, publishId):
  producer = EventHubProducerClient.from_connection_string(
      conn_str=CONNECTION_STR,
      eventhub_name=EVENT_HUB_NAME,
  )
  async with producer:
    await quality_completed_event(producer, correlationId, engagementId, jobRunId, userId, domain, publishId)

###
### Quality Completed Event (Producer + Failure)
###
async def quality_fail_event(producer, correlationId, engagementId, jobRunId, userId, errorLocation, domain, publishId):
    payloadQualityCompleted = json.dumps({
    "id": str(uuid.uuid4()),
    "topic": "TaxDataExchangeServicesDevUS",
    "subject": "Data Service Failed",
    "data":{
        "data": {
        "jobRunId": jobRunId,
        "userId": userId,
        "errorReport": errorLocation,
        "publishId": publishId,
        "status": "Failed"
        },
        "metadata": {
        "correlationId": correlationId,
        "engagementId": engagementId,
        "domain": domain,
        "service": "SDE Data Service"
        }
    },
    "eventType": "SDE.QualityCompleted",
    "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "metadataVersion": "1",
    "dataVersion": "1"
    })

    event_data_batch = await producer.create_batch()
    event_data_quality_completed = EventData(payloadQualityCompleted)
    event_data_quality_completed.properties = {'ClientId': CLIENT_ID}
    event_data_batch.add(event_data_quality_completed)

    await producer.send_batch(event_data_batch)

async def broadcastQualityFailed(correlationId, engagementId, jobRunId, userId, errorLocation, domain, publishId):
  producer = EventHubProducerClient.from_connection_string(
      conn_str=CONNECTION_STR,
      eventhub_name=EVENT_HUB_NAME,
  )
  async with producer:
    await quality_fail_event(producer, correlationId, engagementId, jobRunId, userId, errorLocation, domain, publishId)


###
### Data Available Event (Consumer)
###
async def data_available_event(producer, correlationId, engagementId, jobRunId, publishId, context, dataset, domain):
    payloadDataAvailable = json.dumps({
        "id": str(uuid.uuid4()),
        "topic": "TaxDataExchangeServicesDevUS",
        "subject": "Data Available",
        "data": {
          "data": {
              "jobRunId": jobRunId,
              "publishId": publishId,
              "context": context, #will be state tax filing for SIT from the GTW FLOW,
              "status": "Success",
              "datasets": dataset #updated gold views
          },
          "metadata": {
              "correlationId": correlationId,
              "engagementId": engagementId,
              "domain": domain,
              "service": "SDE Data Service"
          }
        },
        "eventType": "SDE.DataAvailable",
        "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "metadataVersion": "1",
        "dataVersion": "1"
    })

    event_data_batch = await producer.create_batch()
    event_data_available = EventData(payloadDataAvailable)
    event_data_available.properties = {'ClientId': CLIENT_ID}
    event_data_batch.add(event_data_available)

    await producer.send_batch(event_data_batch)

async def broadcastDataAvailable(correlationId, engagementId, jobRunId, publishId, context, dataset, domain):
  producer = EventHubProducerClient.from_connection_string(
      conn_str=CONNECTION_STR,
      eventhub_name=EVENT_HUB_NAME,
  )
  async with producer:
    await data_available_event(producer, correlationId, engagementId, jobRunId, publishId, context, dataset, domain)