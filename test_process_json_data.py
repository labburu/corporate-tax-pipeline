# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Unit Tests

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text(
  name='bronze_location',
  defaultValue='https://xxxxxxxx.blob.core.windows.net/xxxxxx/testing/nocatalogue/US_tax_Global_Tax_Workspace_GTW_s001_2023091319_34/manifest.json',
  label='bronze_location'
)


dbutils.widgets.text(
  name='correlationId',
  defaultValue='f7d40a1b-0c95-491f',
  label='correlationId'
)

dbutils.widgets.text(
  name='SDEengagementId',
  defaultValue='b6a74746-5fcd-4bbf-85e9-',
  label='SDEengagementId'
)

dbutils.widgets.text(
  name='ProducerengagementId',
  defaultValue='132eaf35-0288-4d1f-b6e',
  label='producerengagementID'
)


bronze_location = dbutils.widgets.get('bronze_location')
correlationId = dbutils.widgets.get('correlationId')
producerengagementId = dbutils.widgets.get('ProducerengagementId')
SDEengagementId = dbutils.widgets.get('SDEengagementId')

# COMMAND ----------

# MAGIC %run "../src/BronzeToGoldLoad"

# COMMAND ----------

import unittest



class Testing(unittest.TestCase):
        
  
  def test_manifest_filelist(self):
    ManifestFileList = ReadManifest(SDEengagementId,bronze_location)
    # Simply count the number of rows in the flattened df
    expected_filelist = ["43_BSLA Master data.json","48_EXT Partners Data.json"]
    self.assertEqual(expected_filelist, ManifestFileList, f'Expected {expected_filelist}, but got {ManifestFileList}')


  def test_read_json_data(self):
    path = GetBronzeFolderPath(bronze_location)
    storage_account = GetStorageAccount(bronze_location)
    # print(path)
    df = ReadJsonFiles(SDEengagementId,storage_account,"48_EXT Partners Data.json",bronze_location)
    df.count()
    # Simply count the number of rows in the flattened df
    expected_rows = 31
    actual_rows = df.count()
    self.assertEqual(expected_rows, actual_rows, f'Expected {expected_rows}, but got {actual_rows}')


  def test_curate_json_df(self):
    # print(path)
    storage_account = GetStorageAccount(bronze_location)
    df = ReadJsonFiles(SDEengagementId,storage_account,"48_EXT Partners Data.json",bronze_location)
    transformedDF = TransformDF(producerengagementId,df)
    expected_engagementId = "132eaf35-0288-4d1f-b6"
    actual_engagementId = transformedDF.select(col("_ENGAGEMENTID")).first()[0]
    self.assertEqual(expected_engagementId, actual_engagementId, f'Expected {expected_engagementId}, but got {actual_engagementId}')


  def test_table_existence(self):
    tablename = FileRename("48_EXT Partners Data.json")
    silver_delta_path = GetSilverDeltaPath(SDEengagementId,producerengagementId,tablename,bronze_location)
    silver_table_exists = SilverTableExists(silver_delta_path)
    self.assertEqual(True,silver_table_exists)



suite = unittest.TestLoader().loadTestsFromTestCase(Testing)
runner = unittest.TextTestRunner(verbosity=100)
results = runner.run(suite)
print(results)
assert results.wasSuccessful(), 'Tests Failed, see the logs below for further details'
