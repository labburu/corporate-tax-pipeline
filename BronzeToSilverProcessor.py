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


bronzeLocation = dbutils.widgets.get('location')
correlationId = dbutils.widgets.get('correlationId')
engagementId = dbutils.widgets.get('engagementId')
manifestfileName = dbutils.widgets.get("fileName")
currRunID = dbutils.widgets.get("runID")
currJobID = dbutils.widgets.get("jobID")
sdePublishId = dbutils.widgets.get("sde_publish_id")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import Libraries

# COMMAND ----------

from databricks.sdk.runtime import *
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from delta import *
from DeltaTablesColumnConfigList import *
import json
from utils.BroadcastEvent import *
from utils.StringRename import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Store Job parameters in a json file for other notebooks

# COMMAND ----------


# create dictionary to store parameter values
dataProducer = sdePublishId.split('-')[0]
if dataProducer in producer_json["producer"]:
    JobParameters={
        "location":bronzeLocation,
        "correlationId":correlationId,
        "engagementId":engagementId,
        "RunID":currRunID,
        "JobID":currJobID,
        "sde_publish_id":sdePublishId
    }

    parameterConfigPath = "/Job-Config/parameters.json"
    dbutils.fs.rm(parameterConfigPath)
    dbutils.fs.put(parameterConfigPath,"",True)
    parameters_json = json.dumps(JobParameters, indent=4)
    dbutils.fs.put(parameterConfigPath, parameters_json, overwrite = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze to Silver load

# COMMAND ----------

# MAGIC %run "../sde-gtw/src/BronzeToGoldLoad"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Quality checks

# COMMAND ----------

# MAGIC %run "../sde-gtw/src/SilverDataQualityChecks"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Get Spark configs

# COMMAND ----------

workbench_id = spark.conf.get("spark.sde_workbench_id")
storage_account = spark.conf.get("spark.trusted_storage_account_name")
dataAPI = spark.conf.get("spark.sde_data_api_uri")
# print(workbench_id)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Read and Transform JSON files

# COMMAND ----------

# Job ID and Run ID
currJobRunID = currJobID + "_" +currRunID


# Reading Manifest file
filesToProcess = ReadManifest(workbench_id,bronzeLocation)
if not filesToProcess:
    userId = ExtractUserName(bronzeLocation)
    dataQualityServicers = await broadcastQualityFailed(correlationId, engagementId, currJobRunID, userId, "Manifest file is empty", "STATETAXFILING", sdePublishId)
    raise Exception("Manifest file is empty")
else:
    print(f"List of files to process: {filesToProcess}")


# if data quality checks fail, create error list
errorReportList = []

for file in filesToProcess: #Json files
    failureErrorMsgList = []

    print(f"Processing {file}")
    ## Extract
    bronzeDF = ReadJsonFiles(workbench_id,storage_account,manifestfileName,file,bronzeLocation)
    if len(bronzeDF.columns) == 0 or bronzeDF.count() == 0:
      print("dataframe is empty")
    else:
      # transform
      transformedDF = TransformDF(engagementId,bronzeDF)


      # Validation checks row count
      print(f"Row count validation check for {file}")
      if ValidateRowCount(bronzeDF,transformedDF) == True:
        print(f"Bronze table rows:{bronzeDF.count()},silver table rows:{transformedDF.count()} - Passed: No of  Rows matched between raw and curated dataframe")
      else:
        failedRowCountErrorMsg = f"Bronze table rows:{bronzeDF.count()},silver table rows:{transformedDF.count()} - Failed: Row count Validation failed"
        failureErrorMsgList.append(failedRowCountErrorMsg)
        sendLog(failedRowCountErrorMsg)
        print(failedRowCountErrorMsg)

    
      ## Null check for non-nullable columns
      columnsForNullCheck = files_for_null_check[file]
      print(f"NULL check for {file}: " ,columnsForNullCheck)
      for ncol in columnsForNullCheck:
        df_null_check_count = NullCheckForKeyColumns(ncol,transformedDF)
        if df_null_check_count > 0:
          nullCheckErrorMsg = f"{columnsForNullCheck}: These fields in the landing data are not nullable, but have null values."
          failureErrorMsgList.append(nullCheckErrorMsg)
          sendLog(nullCheckErrorMsg)
          print(f"{ncol} - Null value detected On Non-Nullable Field")
        else:
          print(f"{ncol} - Null check passed for Non-Nullable column")


      ## schema comparison between Json file and Delta table
      jsonDistinctKeys = set()

      for row in bronzeDF.rdd.collect():    
        keys = row.asDict().keys()
        jsonDistinctKeys.update(keys)

      deltatable = FileRename(file)
      silverDeltaPath = GetSilverDeltaPath(workbench_id,storage_account,engagementId,deltatable)
      silver_table_exists = SilverTableExists(silverDeltaPath)
      if silver_table_exists:
          df = spark.read.format("delta").load(silverDeltaPath)
          deltaTableColumnList = df.columns
          schemaDifference = list(set(sorted(jsonDistinctKeys)) - set(deltaTableColumnList))
          if len(schemaDifference) > 0:
              print(f"{deltatable} - Differences found between Json columns and delta table colum  {schemaDifference}")
              schemaMismatchErrorMsg = f"{deltatable} - Differences found between Json columns and delta  table columns {schemaDifference}"
              sendLog(schemaMismatchErrorMsg)
              failureErrorMsgList.append(schemaMismatchErrorMsg)
          else:
              print(f"{deltatable}: Number of columns between Json file and curated delta table match")

    if len(failureErrorMsgList) > 0: 
       failedRowCount = bronzeDF.count()
       errorReportList.append({"file":file,"failedRowCount":failedRowCount,                     "failureReason":failureErrorMsgList})   


## If all data quality checks failed, write to error log and publish quality failed event
if len(errorReportList) > 0:
    errorReportEndpoint = f"{dataAPI}error-report?engagementId={engagementId}&correlationId={correlationId}"
    errorList_json = json.dumps(errorReportList,indent=4)
    userId = ExtractUserName(bronzeLocation)
    errorReportPath = f"abfss://{workbench_id}@{storage_account}.dfs.core.windows.net/error-log/{engagementId}/{correlationId}/error.json"
    if ErrorReportPathExists(errorReportPath):
      dbutils.fs.rm(errorReportPath)
      dbutils.fs.put(errorReportPath, "", overwrite=True)
      dbutils.fs.put(errorReportPath, errorList_json, overwrite=True)
    else:
      dbutils.fs.put(errorReportPath, "")
      dbutils.fs.put(errorReportPath, errorList_json, overwrite = True)

    dataQualityServicers = await broadcastQualityFailed(correlationId, engagementId, currJobRunID, userId, errorReportEndpoint, "STATETAXFILING", sdePublishId)
    raise Exception("Data quality checks failed - Error condition met. Task failed")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Load Delta tables

# COMMAND ----------

print(f"List of files to process: {filesToProcess}")

# if load is successful list the tables processed in silver layer
deltaTablesProcessedList = []


# If merge operation fails
errorReportList = []

for file in filesToProcess: #Json files
  failureErrorMsgList = []
  print(f"Processing {file}")
  ## Extract
  bronzeDF = ReadJsonFiles(workbench_id,storage_account,manifestfileName,file,bronzeLocation)
  if len(bronzeDF.columns) == 0 or bronzeDF.count() == 0:
    print("dataframe is empty")
  else:
    # transform
    transformedDF = TransformDF(engagementId,bronzeDF)
    # Load
    tablename = FileRename(file)
    silver_delta_path = GetSilverDeltaPath(workbench_id,storage_account,engagementId,tablename)
    silver_table_exists = SilverTableExists(silver_delta_path)
    if not silver_table_exists: 
      ProcessedTable = LoadIntoSilverOverwrite(transformedDF,tablename,silver_delta_path)
      deltaTablesProcessedList.append(ProcessedTable)
    else:
        col_list = silver_tables_join_columns[file]
        UpdatedTable = LoadIntoSilverMerge(transformedDF,tablename,silver_delta_path,col_list)
        if UpdatedTable == tablename: 
            deltaTablesProcessedList.append(UpdatedTable)
        else:
            mergeErrorMsg = UpdatedTable
            failedRowCount = bronzeDF.count()
            errorReportList.append({"file":file,"failedRowCount":failedRowCount,"failureReason":mergeErrorMsg})
            

if len(errorReportList) > 0:
    # Add to error log
    errorReportEndpoint = f"{dataAPI}error-report?engagementId={engagementId}&correlationId={correlationId}"
    errorList_json = json.dumps(errorReportList,indent=4)
    userId = ExtractUserName(bronzeLocation)
    errorReportPath = f"abfss://{workbench_id}@{storage_account}.dfs.core.windows.net/error-log/{engagementId}/{correlationId}/error.json"
    if ErrorReportPathExists(errorReportPath):
      dbutils.fs.rm(errorReportPath)
      dbutils.fs.put(errorReportPath, "", overwrite=True)
      dbutils.fs.put(errorReportPath, errorList_json, overwrite=True)
    else:
      dbutils.fs.put(errorReportPath, "")
      dbutils.fs.put(errorReportPath, errorList_json, overwrite = True)
    dataQualityServicers = await broadcastQualityFailed(correlationId, engagementId, currJobRunID, userId, errorReportEndpoint, "STATETAXFILING", sdePublishId)
    raise Exception(f"Error during merge operation. Task Failed - {errorList_json}")             


# COMMAND ----------

# MAGIC %md
# MAGIC # Merge validation in silver layer using Operation metrics

# COMMAND ----------


if len(deltaTablesProcessedList) > 0:
    print(f"Delta tables load complete for {deltaTablesProcessedList}")

errorReportList = []
failedValidationTableList = []

for table in deltaTablesProcessedList:
  print(f"Validating {table}")
#   tablename = FileRename(table)
  silver_delta_path = GetSilverDeltaPath(workbench_id,storage_account,engagementId,table)
  silver_table_exists = SilverTableExists(silver_delta_path)
  if silver_table_exists: 
    booleanRowsMatch = merge_validation(silver_delta_path)[0]
    operationValue = merge_validation(silver_delta_path)[1]


    if booleanRowsMatch == "False" and operationValue == "MERGE":
      failedValidationTableList.append(table) 
      df = spark.read.format("delta").load(silver_delta_path)
      latesdtLoadDate = df.agg(max("_MODIFIEDDATE")).collect()[0][0]  
      latestLoadedDataDF = df.filter(col("_MODIFIEDDATE") == latesdtLoadDate)
      failedRowCount = latestLoadedDataDF.count()
      errorMsg = f"Error: Data validation failed for {table} - Mismatch between Number of rows landing data Vs rows inserted or updated."
      errorReportList.append({"file":table,"failedRowCount":failedRowCount,"failureReason":errorMsg})
      

# If merge validation fails
if len(errorReportList) > 0:
    errorReportPath = f"abfss://{workbench_id}@{storage_account}.dfs.core.windows.net/error-log/{engagementId}/{correlationId}/error.json"
    userId = ExtractUserName(bronzeLocation)
    if ErrorReportPathExists(errorReportPath):  # check if error log file exists
        try: 
          existingErrorContent = dbutils.fs.head(errorReportPath)    
          existingErrorList = json.loads(existingErrorContent)
          MergeErrorList = json.dumps(errorReportList, indent=4)
          existingErrorList.append(MergeErrorList) #if error log file exists append new error list
          dbutils.fs.put(errorReportPath, existingErrorList, overwrite = True) # after appending MergeErrorList, update the error-log
        except json.JSONDecodeError as e:
            sendLog(f"error parsing JSON: {str(e)}")
            dbutils.notebook.exit(str(e))         
    else:
      dbutils.fs.put(errorReportPath, "")
      MergeErrorList = json.dumps(errorReportList, indent=4)
      dbutils.fs.put(errorReportPath, MergeErrorList, overwrite = True)

    
    errorReportEndpoint = f"{dataAPI}error-report?engagementId={engagementId}&correlationId={correlationId}"
    errorMsg = f"{failedValidationTableList} - Mismatch between Number of rows in landing data Vs rows inserted or updated."
    sendLog(errorMsg)
    dataQualityServicers = await broadcastQualityFailed(correlationId, engagementId, currJobRunID, userId,errorReportEndpoint, "STATETAXFILING",sdePublishId)
    raise Exception(f"Merge operation failed. Task Failed - {errorMsg}")
    # dbutils.notebook.exit(errorMsg)
else:
  print("Merge operaton successful in silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create silver tables in SQL warehouse

# COMMAND ----------

print("Tables for load as SQL objects: ", deltaTablesProcessedList)

dataProducer = sdePublishId.split('-')[0]
if dataProducer in producer_json["producer"]:
    schema = producer_json["schema"]
    # Create schema if it doesn't exist 
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS hive_metastore.{schema} COMMENT 'This is a database for {schema}' """)

for sqltable in deltaTablesProcessedList: 
  sql_table_exists = databricksTableExists(sqltable,schema)
  silver_delta_path = GetSilverDeltaPath(workbench_id,storage_account,engagementId,sqltable)
  if not sql_table_exists:
    print(f"Creating table {schema}.{sqltable}...")
    spark.sql(f""" CREATE TABLE IF NOT EXISTS {sqltable} USING DELTA LOCATION '{silver_delta_path}' """) # table with new rows referenced to delta lake path
    spark.sql(f""" CREATE TABLE IF NOT EXISTS {schema}.{sqltable} AS SELECT * FROM {sqltable} """) # create table under schema
  else: 
    print(f"Updating table {schema}.{sqltable}...") 
    spark.sql(f""" DELETE FROM {schema}.{sqltable} WHERE _ENGAGEMENTID = '{engagementId}' """)
    spark.sql(f""" INSERT INTO {schema}.{sqltable} SELECT * FROM {sqltable} """) # insert new rows into statetaxfiling schema from updated table under default schema referenced to curate delta lake path post merge

