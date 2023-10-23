# Databricks notebook source
from pyspark.sql import *
import pyspark.sql.functions as f
# from pyspark.sql.functions import *
from utils.StringRename import *
from utils.BroadcastEvent import *
from DeltaTablesColumnConfigList import *



# COMMAND ----------

# MAGIC %run "../sde-gtw/src/BronzeToGoldLoad"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # GET Current JOB parameters

# COMMAND ----------

parameters_path = "/Job-Config/parameters.json"
file_content = dbutils.fs.head(parameters_path)
try:
    parameters_json_data = json.loads(file_content)
    print(parameters_json_data)
except json.JSONDecodeError as e:
    print(f"Error parsing JSON: {str(e)}")

# print(parameters_json_data["producer"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create GOLD Views

# COMMAND ----------


dataProducer = parameters_json_data["sde_publish_id"].split('-')[0]

# List of views created in the current run
createdViewsList = []

if dataProducer in producer_json["producer"]:
    schema = producer_json["schema"]
    sqlTableList = getSQLTableViews(dataProducer,schema,"MANAGED")
    print(f"Tables available in silver layer: {sqlTableList}")

    for sqltable in sqlTableList:
        viewName = "vw" + "_" + sqltable
        view_exists = databricksViewExists(viewName, schema)
        if not view_exists:
            if viewName != "vw_irsformdata":
                print(f"Creating View {schema}.{viewName}...")
                spark.sql(f"""CREATE OR REPLACE VIEW {schema}.{viewName} AS SELECT * FROM {schema}.{sqltable}""")
                createdViewsList.append(viewName)
            else:
                spark.sql(f"""CREATE OR REPLACE VIEW {schema}.vw_irsformdata AS SELECT TAX_YEAR as TaxYear, LEID as LegalEntityID, TAX_YEAR_BEGIN as ReportingStartDate, TAX_YEAR_END as ReportingEndDate, FILING_GROUP as FilingGroupId, ELEMENT_NAME as IRSElementName, VALUE as Value, _ENGAGEMENTID FROM {schema}.irsformdata""")
                createdViewsList.append(viewName)


if len(createdViewsList) > 0:
    print(f"List of Views created or updated: {createdViewsList} under schema {schema}")
else:
    print("No New Views created")

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast Event

# COMMAND ----------

# If all the above commands run successfully, Quality complete and Data availble events are published

correlationId = parameters_json_data["correlationId"]
engagementId = parameters_json_data["engagementId"]
# Job ID and Run ID
currJobRunID = parameters_json_data["JobID"] + "_" +parameters_json_data["RunID"]
userId = ExtractUserName(parameters_json_data["location"])
sdePublishId = parameters_json_data["sde_publish_id"]


dataQualityServicers = await broadcastQualityCompleted(correlationId, engagementId, currJobRunID, userId, "STATETAXFILING", sdePublishId)

dataAvailableServicers = await broadcastDataAvailable(correlationId, engagementId, currJobRunID,sdePublishId, "StateTaxFiling", createdViewsList, "STATETAXFILING")

