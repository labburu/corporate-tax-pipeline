# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Get Variables From Azure Function

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text(
  name='location',
  defaultValue='',
  label='location'
)


dbutils.widgets.text(
  name='fileName',
  defaultValue='',
  label='fileName'
)


dbutils.widgets.text(
  name='correlationId',
  defaultValue='',
  label='correlationId'
)

dbutils.widgets.text(
  name='engagementId',
  defaultValue='',
  label='engagementId'
)


dbutils.widgets.text(
  name='runID',
  defaultValue='',
  label='runID'
)

dbutils.widgets.text(
  name='jobID',
  defaultValue='',
  label='jobID'
)

dbutils.widgets.text(
  name='sde_publish_id',
  defaultValue='',
  label='sde_publish_id'
)


fileLocation = dbutils.widgets.get('location')
correlationId = dbutils.widgets.get('correlationId')
engagementId = dbutils.widgets.get('engagementId')
# fileName = dbutils.widgets.get("fileName")
currRunID = dbutils.widgets.get("runID")
currJobID = dbutils.widgets.get("jobID")
sdePublishId = dbutils.widgets.get("sde_publish_id")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import libraries

# COMMAND ----------

from databricks.sdk.runtime import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime, timezone
from delta import *
import json
import os
from columnListConfig import *
from utils.BroadcastEvent import *
from utils.AppInsights import *
from utils.StringRename import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Get Spark configs

# COMMAND ----------

workbenchId = spark.conf.get("spark.sde_workbench_id")
storageAccount = spark.conf.get("spark.trusted_storage_account_name")
dataAPI = spark.conf.get("spark.sde_data_api_uri")
fileName = os.path.split(fileLocation)[1]
print(fileName)

# COMMAND ----------

# MAGIC %run "../src/BronzeToGoldLoad"

# COMMAND ----------

# MAGIC %run "../src/SilverDataQualityChecks"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create custom schema for beacon files

# COMMAND ----------

def createStructTypeSchema(json_schema):
    jsonFields = []
    for field, data_type in expectedDataType.items():
        if data_type == "string":
            jsonField = StructField(field, StringType(), nullable=True)
        elif data_type == "int":
            jsonField = StructField(field, IntegerType(), nullable=True)
        elif data_type == "decimal(26,6)":
            jsonField = StructField(field, DecimalType(26,6), nullable=True)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
        jsonFields.append(jsonField)
    return StructType(jsonFields)

beaconSchema = createStructTypeSchema(expectedDataType)
# print(beaconSchema)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Read and Transform JSON files

# COMMAND ----------

# Job ID and Run ID
currJobRunID = currJobID + "_" +currRunID

beaconRawFilePath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/{fileLocation}"

JsonDF = spark.read.format("json").option("multiline", "true").schema(beaconSchema).load(beaconRawFilePath)
# Add metadata properties
modifiedDate = datetime.now(timezone.utc).timestamp()
formattedModifiedDate = datetime.fromtimestamp(modifiedDate)
transformedJsonDF = JsonDF.withColumn("_MODIFIEDDATE",lit(formattedModifiedDate)).cache()
transformedJsonDF = transformedJsonDF.withColumn("_ENGAGEMENTID",lit(engagementId)).cache()
transformedJsonDF = transformedJsonDF.withColumn("_CORRELATIONID",lit(correlationId)).cache()
transformedJsonDF = transformedJsonDF.withColumn("_FILEPATH",lit(fileLocation)).cache()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data quality checks

# COMMAND ----------


# if data quality checks fail, create error list
errorReportList = []

## Data type Validation
# get column data type list from Json file
bronzeDFColumnDataType = {}
for column_name, data_type in JsonDF.dtypes:
    bronzeDFColumnDataType[column_name] = data_type


datatypeDifferences = {}

for key,value in expectedDataType.items():
    if bronzeDFColumnDataType[key] != value:
        datatypeDifferences[key] = {"expectedDataType":value, "bronzeDFColumnDataType":bronzeDFColumnDataType[key]}

if len(datatypeDifferences) > 0:
    dataTypeDifferenceErrorMsg = f"Differences in data type for specific fields found from what is expected : {datatypeDifferences}"
    print(datatypeDifferences)
    failedRowCount = JsonDF.count()
    errorReportList.append({"file":fileName,"failedRowCount":failedRowCount,"failureReason":dataTypeDifferenceErrorMsg})
else:
    print("Data type validations passed")



## Check for duplicates
groupByColumns = groupby_columns["column_list"]
duplicateRowsDF = JsonDF.groupBy(*groupByColumns).count().filter(col("count") > 1)
if duplicateRowsDF.count() > 0:
    failedRowCount = JsonDF.count()
    duplicatesErrorMsg = f"{fileName}: There are duplicates in the file"
    sendLog(duplicatesErrorMsg)
    print(duplicatesErrorMsg)
    errorReportList.append({"file":fileName,"failedRowCount":failedRowCount,"failureReason":duplicatesErrorMsg})
else:
    print("There are no duplicate records, validation complete")


## Null check for non-nullable columns
columnsForNullCheck = columns_for_null_check["column_list"]
print(f"NULL check for {fileName}: ",columnsForNullCheck)
for ncol in columnsForNullCheck:
  df_null_check_count = NullCheckForKeyColumns(ncol,JsonDF)
  if df_null_check_count > 0:
    nullCheckErrorMsg = f"{columnsForNullCheck}: These fields in the landing data are not nullable but have null values."
    sendLog(nullCheckErrorMsg)
    print(nullCheckErrorMsg)
    failedRowCount = JsonDF.count()
    errorReportList.append({"file":fileName,"failedRowCount":failedRowCount,"failureReason":nullCheckErrorMsg})
  else:
    print(f"{ncol} - Null check passed for Non-Nullable column")
        


## schema comparison between Json file and Silver table
jsonDistinctKeys = set()
for row in JsonDF.rdd.collect():    
  keys = row.asDict().keys()
  jsonDistinctKeys.update(keys)

expectedDeltaTableColumnList = schema_validation_columns["column_list"]
schemaDifference = list(set(sorted(jsonDistinctKeys)) - set(expectedDeltaTableColumnList))
if len(schemaDifference) > 0:
    schemaMismatchErrorMsg = f"{fileName} - Differences in schema found in the file from what is expected {schemaDifference}"
    print(schemaMismatchErrorMsg)
    failedRowCount = JsonDF.count()
    errorReportList.append({"file":fileName,"failedRowCount":failedRowCount,"failureReason":schemaMismatchErrorMsg})
else:
    print(f"{fileName}: Schema validation passed")


## If all data quality checks failed, write to error log and publish quality failed event
if len(errorReportList) > 0:
    errorReportEndpoint = f"{dataAPI}error-report?engagementId={engagementId}&correlationId={correlationId}"
    errorList_json = json.dumps(errorReportList,indent=2)
    errorReportPath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/error-log/{engagementId}/{correlationId}/error.json"
    userId = ExtractUserName(fileLocation)
    if ErrorReportPathExists(errorReportPath):
      dbutils.fs.rm(errorReportPath)
      dbutils.fs.put(errorReportPath, "", overwrite=True)
      dbutils.fs.put(errorReportPath, errorList_json, overwrite=True)
    else:
      dbutils.fs.put(errorReportPath, "")
      dbutils.fs.put(errorReportPath, errorList_json, overwrite = True)
    #TODO sdePublishId value is subject to change
    dataQualityServicers = await broadcastQualityFailed(correlationId,engagementId, currJobRunID, userId, errorReportEndpoint, "BEACON", sdePublishId)
    raise Exception("Data quality checks failed - Error condition met. Task failed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Write data to bronze layer

# COMMAND ----------

bronzeFilePath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/bronze/federal_domestic_sourcing"

if not bronzeFilePath:
    transformedJsonDF.write.format("delta").mode("overwrite").partitionBy("_ENGAGEMENTID").save(bronzeFilePath)
else:
    transformedJsonDF.write.format("delta").mode("append").partitionBy("_ENGAGEMENTID").save(bronzeFilePath)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Transform Bronze delta table and load into silver layer

# COMMAND ----------

bronzeFilePath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/bronze/federal_domestic_sourcing"

bronzeDF = spark.read.format("delta").load(bronzeFilePath)
filteredBronzeDF = bronzeDF.filter(col("_CORRELATIONID") == correlationId)  # filtered on current transaction row
transformedBronzeDF = filteredBronzeDF.withColumn("_SDEPUBLISHID",lit(sdePublishId)).cache()

# COMMAND ----------

silverDeltaPath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/curated/federal_domestic_sourcing"

if not silverDeltaPath:
    transformedBronzeDF.write.format("delta").mode("overwrite").partitionBy("_ENGAGEMENTID").save(silverDeltaPath)
else:
    transformedBronzeDF.write.format("delta").mode("append").partitionBy("_ENGAGEMENTID").save(silverDeltaPath)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create silver tables in SQL warehouse

# COMMAND ----------


# Create schema if it doesn't exist 
spark.sql(f""" CREATE SCHEMA IF NOT EXISTS hive_metastore.beacon COMMENT 'This is a database for gtw-beacon data flow' """)

sqlTableExists = databricksTableExists("federal_domestic_sourcing","beacon")
silverDeltaPath = GetSilverDeltaPath(workbenchId,storageAccount,engagementId,"federal_domestic_sourcing")
if not sqlTableExists:
  print("Creating table beacon.federal_domestic_sourcing...")
  spark.sql(f""" CREATE TABLE beacon.federal_domestic_sourcing USING DELTA LOCATION '{silverDeltaPath}' """) # table with new rows referenced to delta lake path


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Gold views

# COMMAND ----------


viewExists = databricksTableExists("vw_federal_domestic_sourcing","beacon")
if not viewExists:
    print(f"Creating View beacon.vw_federal_domestic_sourcing...")
    spark.sql(f"""CREATE VIEW beacon.vw_federal_domestic_sourcing AS SELECT ACCOUNT, AMOUNT, BASE_BASKET, BASE_ENTITY_LEVEL, BASE_FDII, BASE_LEVEL, BASE_TRANS_CATG_NAME, BASKET_CODE, CONSOL_GROUP_KEY,
    ENTITY_KEY, FDII_CODE, GROUP_OBJ_KEY, HO_LEID, HO_REPORTING_PERIOD, HO_TAX_CODE, INTL_ACCT_CATEGORY, INTL_ACCT_SUB_CATEGORY, IS_DIRTY, ISSUE_ID, LEID, ME_CODE, METHOD, PARENT_ME_CODE, REPORTING_PERIOD, SIC_CODE, TAX_CODE, TAX_YEAR, TRANS_CATG_NAME, TRANSACTION_KEY, _MODIFIEDDATE, _ENGAGEMENTID, _CORRELATIONID, _SDEPUBLISHID FROM beacon.federal_domestic_sourcing""")
    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Validation post gold layer processing

# COMMAND ----------


## Schema validation between Json file and silver delta table
jsonDistinctKeys = set()

for row in JsonDF.rdd.collect():    
  keys = row.asDict().keys()
  jsonDistinctKeys.update(keys)
sortedJsonDistinctKeys = sorted(jsonDistinctKeys)

silverDeltaPath = f"abfss://{workbenchId}@{storageAccount}.dfs.core.windows.net/curated/federal_domestic_sourcing"
if silverDeltaPath:
    df = spark.read.format("delta").load(silverDeltaPath)
    deltaTableColumnList = df.columns
    filteredColumnList = [item for item in deltaTableColumnList if not item.startswith("_")]
    schemaDifference = list(set(filteredColumnList) - set(sortedJsonDistinctKeys))
    if len(schemaDifference) > 0:
        schemaMismatchErrorMsg = f"federal_domestic_sourcing - Differences found between Json columns and delta  table columns {schemaDifference}"
        raise Exception(schemaMismatchErrorMsg)
    else:
        print("federal_domestic_sourcing: Number of columns between Json file and curated delta table match")



## Validate row count
# bronze table row count
bronzeDFRowCount = bronzeDF.count()

# sql table row count 
SQLtableDF = spark.sql(f"select count(*) from beacon.federal_domestic_sourcing where _ENGAGEMENTID = '{engagementId}'")
SQLTableRowCount = SQLtableDF.head()[0]

if SQLTableRowCount != bronzeDFRowCount:
    raise Exception(f"Row count does not match, validation failed- Bronze table row count: {bronzeDFRowCount}, Silver table row count: {SQLTableRowCount}")
else:
    print(f"Row count validation passed- Bronze table row count: {bronzeDFRowCount}, Silver table row count: {SQLTableRowCount}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast Event

# COMMAND ----------

# If all the above commands run successfully, Quality complete and Data availble events are published

userId = ExtractUserName(fileLocation)
dataQualityServicers = await broadcastQualityCompleted(correlationId, engagementId, currJobRunID, userId, "BEACON", sdePublishId)

dataAvailableServicers = await broadcastDataAvailable(correlationId, engagementId, currJobRunID,sdePublishId, "BEACON", ["vw_federal_domestic_sourcing"], "BEACON")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- for Full reload
# MAGIC -- drop schema beacon cascade
# MAGIC -- drop table beacon.federal_domestic_sourcing
# MAGIC -- drop view beacon.vw_federal_domestic_sourcing
# MAGIC
# MAGIC -- select * from beacon.federal_domestic_sourcing where _ENGAGEMENTID = 'bd07c01c-ed22-44ee-8469-5bd1f8b1bb51'
