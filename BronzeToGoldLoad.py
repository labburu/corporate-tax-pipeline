# Databricks notebook source
## Import libraries

from databricks.sdk.runtime import *
from utils.AppInsights import *
from utils.StringRename import *
from datetime import datetime, timezone
from pyspark.sql import *
import pyspark.sql.functions as f
from delta import *
import json




# COMMAND ----------



def GetBronzeFolderPath(location,manifestfilename):
  bronze_folder_path = location.rsplit(manifestfilename)[0]
  bronze_folder_levels = re.search(r'nocatalogue/(.*)', bronze_folder_path)
  bronze_path_folders_split = bronze_folder_levels.group(0).split("/")
  bronze_file_path = '/'.join(bronze_path_folders_split)
  return bronze_file_path[:-1]
  

def GetSilverDeltaPath(container,storage_account,engagementId,deltatable):
  silver_delta_path = f'abfss://{container}@{storage_account}.dfs.core.windows.net/curated/{deltatable}'
  return silver_delta_path


def SilverTableExists(table_path):
  try:
    dbutils.fs.ls(table_path)
  except:
    return False
  return True


def databricksTableExists(tableName,schemaName='default'):
  return spark.sql(f"show tables in {schemaName} like '{tableName}'").count()==1


def ErrorReportPathExists(report_path):
  try:
    dbutils.fs.ls(report_path)
  except:
    return False
  return True


def getSQLTableViews(producer,schema,table_type):
    tables_and_views = spark.catalog.listTables(schema)
    get_sql_table_views = [table.name for table in tables_and_views if table.tableType == table_type]
    return get_sql_table_views


def getSQLTableViewColumnList(producer,schema,table):
    table_name = f"{schema}.{table}"
    columns = spark.catalog.listColumns(table_name)
    column_names = [column.name for column in columns]
    return column_names


def databricksViewExists(viewName,schemaName='default'):
    view_name = f"{schemaName}.{viewName}"
    view_exists = spark.catalog.tableExists(view_name)
    return view_exists

# COMMAND ----------




# Read Manifest file
def ReadManifest(container,location):
  try:
    manifest_file_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{location}"
    ManifestDF = spark.read.format("json").option("multiline","true").option("inferSchema", "true").load(manifest_file_path)
    sorted_ManifestDF = ManifestDF.orderBy(['order_of_data'],ascending = [True])
    FileList = sorted_ManifestDF.rdd.map(lambda x: x.fileName).collect()
    return FileList
  except Exception as e:
    sendLog(f"Manifest file Read operation failed - {str(e)}")



def ReadJsonFiles(container,storage_account,manifestfilename,filename,location):
  try:
    bronze_folder_path = GetBronzeFolderPath(location,manifestfilename)
    # storage_account = GetStorageAccount(bronze_location)
    json_file_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{bronze_folder_path}/{filename}"
    bronzeDF = spark.read.format("json").option("multiline","true").option("inferSchema", "true").load (json_file_path)
    return bronzeDF  
  except Exception as e:
    sendLog(f"Json file Read operation failed - {str(e)}")
    return None 


def TransformDF(engagementId, bronzedf):
  try:
    # bearerToken = getBearerToken()
    # clientId = getClientID(bearerToken,engagementId)
    modifiedDate = datetime.now(timezone.utc).timestamp()
    formattedModifiedDate = datetime.fromtimestamp(modifiedDate)
    TransformedDF = bronzedf.withColumn("_MODIFIEDDATE",lit(formattedModifiedDate)).cache()
    TransformedDF = TransformedDF.withColumn("_ENGAGEMENTID",lit(engagementId)).cache()
    # TransformedDF = TransformedDF.withColumn("_CLIENTID",lit(clientId)).cache()
    return TransformedDF
  except Exception as e:
    sendLog(f"Json file Transformation failed - {str(e)}")
    return None 
  
## If table does not exist perform overwrite
def LoadIntoSilverOverwrite(transformeddf,tablename,silver_delta_path):
  try:
    transformeddf.write.format("delta").mode("overwrite").partitionBy("_ENGAGEMENTID").save(silver_delta_path)
    print(f'{tablename} loaded successfully')
    return tablename
  except Exception as e:
    sendLog(f"silver table loading failed - {str(e)}")
    return None
  

## If table exists perform merge or upsert 
def LoadIntoSilverMerge(transformeddf,tablename,silver_delta_path,join_columns:list):
  try:
    deltaTableDF = spark.read.format("delta").load(silver_delta_path)
    target_table = "target_table"
    source_alias = "source"
    deltaTableDF.createOrReplaceTempView(target_table)
    transformedDF.createOrReplaceTempView(source_alias)
    merge_query = f"""
    MERGE INTO {target_table} AS target
    USING {source_alias} AS source
    ON { " AND ".join(f"target.{col_name} = source.{col_name}" for col_name in join_columns) }
    WHEN MATCHED THEN
      DELETE
    WHEN NOT MATCHED THEN
      INSERT *
    """
    # Execute merge query
    spark.sql(merge_query)

    mainDeltaTable = spark.table(target_table)
    mainDeltaTable.write.format("delta").mode("overwrite").partitionBy("_ENGAGEMENTID").save(silver_delta_path)

    return tablename     
  except Exception as e:
    errorMsg = f"Error during merge operation - {str(e)}"
    sendLog(errorMsg)
    return errorMsg


# Delta merge validation
def merge_validation(silver_delta_path):
  deltaTableMain = DeltaTable.forPath(spark, silver_delta_path)
  currentOperationDF = deltaTableMain.history(1)
  currentOperationDF.createOrReplaceTempView("OperationMetrics")

  resultDF = spark.sql("select timestamp, operation, operationMetrics.numSourceRows, operationMetrics.numTargetRowsInserted, operationMetrics.numTargetRowsUpdated, operationMetrics.numTargetRowsMatchedDeleted, iff(operationMetrics.numTargetRowsInserted+ operationMetrics.numTargetRowsUpdated+ operationMetrics.numTargetRowsMatchedDeleted = operationMetrics.numSourceRows ,'True','False') as ifRowsMatch from operationmetrics")

  booleanRowsMatch = resultDF.select(col("ifRowsMatch")).first()[0]
  operationValue = resultDF.select(col("operation")).first()[0]

  return booleanRowsMatch , operationValue

# COMMAND ----------

# utility function to get table list for dropping tables in a schema
def getFilteredTableList(schema,filter_pattern):
    tableList = spark.sql(f"SHOW TABLES in {schema}")
    tableNames = [row.tableName for row in tableList.collect()]
    filteredTableList = [item for item in tableNames if not any(item.startswith(prefix) for prefix in filter_pattern)]
    return filteredTableList
