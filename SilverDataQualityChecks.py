# Databricks notebook source
# Data Validations
from databricks.sdk.runtime import *
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *



def ValidateRowCount(SourceDF,TargetDF):
    return SourceDF.count() == TargetDF.count()


def NullCheckForKeyColumns(column_name,TargetDF):
  df_null_check_count = TargetDF.filter(col(column_name)== "")
  return df_null_check_count.count()


def SchemaDiff(DF1, DF2):
  # Getting schema for both dataframes in a dictionary
  DF1Schema = {x[0]:x[1] for x in DF1.dtypes}
  DF2Schema = {x[0]:x[1] for x in DF2.dtypes}

    
   # Column present in DF1 but not in DF2
  DF1MinusDF2 = dict.fromkeys((set(DF1.columns) - set(DF2.columns)), '')
  for column_name in DF1MinusDF2:
    DF1MinusDF2[column_name] = DF1Schema[column_name]
  

  # Column present in DF2 but not in DF1
  DF2MinusDF1 = dict.fromkeys((set(DF2.columns) - set(DF1.columns)), '')
  for column_name in DF2MinusDF1:
    DF2MinusDF1[column_name] = DF2Schema[column_name]
  
  # Find data type changed in DF1 as compared to DF2
  UpdatedDF1Schema = {k:v for k,v in DF1Schema.items() if k not in DF1MinusDF2}
  UpdatedDF1Schema = {**UpdatedDF1Schema, **DF2MinusDF1}
  DF1DataTypesChangedFrom = {}
  DF1DataTypesChangedTo = {}
  for column_name in UpdatedDF1Schema:
      if UpdatedDF1Schema[column_name] != DF2Schema[column_name]:
        DF1DataTypesChangedFrom[column_name] = DF2Schema[column_name]
        DF1DataTypesChangedTo[column_name] = DF1Schema[column_name]

  if len(DF1DataTypesChangedFrom) >0 or len(DF1DataTypesChangedTo) > 0:
    print(f'data type change from {DF1DataTypesChangedFrom} to {DF1DataTypesChangedTo}')

  return DF1MinusDF2, DF2MinusDF1, DF1DataTypesChangedFrom, DF1DataTypesChangedTo


# Delta merge validation
def merge_validation(silver_delta_path):
  deltaTableMain = DeltaTable.forPath(spark, silver_delta_path)
  currentOperationDF = deltaTableMain.history(1)
  currentOperationDF.createOrReplaceTempView("OperationMetrics")

  resultDF = spark.sql("select timestamp, operation, operationMetrics.numSourceRows, operationMetrics.numTargetRowsInserted, operationMetrics.numTargetRowsUpdated, operationMetrics.numTargetRowsMatchedDeleted, iff(operationMetrics.numTargetRowsInserted+ operationMetrics.numTargetRowsUpdated+ operationMetrics.numTargetRowsMatchedDeleted = operationMetrics.numSourceRows ,'True','False') as ifRowsMatch from operationmetrics")

  booleanRowsMatch = resultDF.select(col("ifRowsMatch")).first()[0]
  operationValue = resultDF.select(col("operation")).first()[0]

  return booleanRowsMatch , operationValue
