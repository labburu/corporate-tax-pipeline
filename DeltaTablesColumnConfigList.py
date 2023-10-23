

silver_tables_join_columns = {
"40_PDF Metadata Service.json":["TAX_YEAR","ENTITY","CDR_NO","REPORTING_PERIOD"],
"41_Consolidated Amount Transactions.json":["TAX_YEAR","LEID","CDR_NO","REPORTING_PERIOD","BSLA_CODE","FILING_GROUP","FIR_ACCT","FORM_TAC","IRS_FORM_NO","LINE_NO","SCHDM_ID","SCHDM_USER_DESC","LAST_USER_UPDATE","LAST_DATE_UPDATE"],
"42_LE Main Tx.json": ["TAX_YEAR","LEID","CDR_NO","TAX_YEAR_BEGIN","TAX_YEAR_END","REPORTING_PERIOD","FUNC_CURR"],
"43_BSLA Master data.json": ["TAX_YEAR","BSLA_CODE"],
"44_4562 Details View.json": ["TAX_YEAR","LEID","CDR_NO","BSLA_CODE","LINE_NO","SUB_LINE_NO","PROPERTY_DESC","RECOVERY_PERIOD","BUSINESS_PERCENT","CONVENTION","METHOD","DATE_PLACED_SERVICE","ENTITY_TYPE","ISSUE_ID"],
"45_M2 Details Data.json":["TAX_YEAR","LEID","CDR_NO","REPORTING_PERIOD","BSLA_CODE","SCENARIO"],
"46_ScheduleC Details View.json":["TAX_YEAR","LEID","CDR_NO","BSLA_CODE","REPORTING_PERIOD","ENTITY_TYPE","SCENARIO","LINE_NO"],
"47_PSHIP PRT Ratios.json":["TAX_YEAR","PSHIP_LEID","PSHIP_CDR_NO","PSHIP_REPORTING_PERIOD","PSHIP_BSLA","PSHIP_CURRENCY","PARTNER_LEID","PARTNER_CDR_NO","PARTNER_REPORTING_PERIOD","POSTING_PARTNER_LEID","POSTING_PARTNER_CDR_NO","POSTING_PARTNER_RP"],
"48_EXT Partners Data.json":["TAX_YEAR","PTR_LEID","PTR_CDR_NO","PTR_REPORTING_PERIOD"],
"49_IRS Form Data.json":["TAX_YEAR","LEID","CDR_NO","REPORTING_PERIOD","SCENARIO","JOB_ID","FILING_GROUP","CONTENT_SOURCE","ELEMENT_NAME","SNO","ENTITY_TYPE","IRS_FORM_NO","SCHEDULE","SCHEDULE","PART_NO","SECTION_NAME","LINE_NO","COL_ID","WPD_Y_N","PARENT_ELEMENT_NAME"],
"50_1065 Data.json":["TAX_YEAR","LEID","CDR_NO","REPORTING_PERIOD","FORM","SCHEDULE","LINE","FORM_COLUMN"],
"51_1065 Textual Info.json":["TAX_YEAR","IRS_FORM_NO","LEID","CDR_NO","REPORTING_PERIOD","DESCRIPTION"],
"64_TDIR LE BSLA Assign.json":["TAX_YEAR","LEID","CDR_NO","BSLA_CODE","SCENARIO","REPORTING_PERIOD"],
"65_GTW Dividends Payor Details.json":["TAX_YEAR","LEID","CDR_NO","REPORTING_PERIOD","ME_CODE","PARENT_ME_CODE","ENTITY_TYPE","FILING_GROUP","PAYOR_LEID","PAYOR_CDR_NO","PAYOR_RP","ADJ_SOURCE_TYPE","SYSTEM_ACCT","FORM","SCHEDULE","LINE","LE_TAX_TYPE","HO_LEID","HO_CDR_NO","HO_REPORTING_PERIOD","PAYOR_TAX_TYPE","LE_NAME"]                              
}


files_for_null_check = {
  "40_PDF Metadata Service.json" : ["TAX_YEAR"],
  "41_Consolidated Amount Transactions.json" : ["TAX_YEAR","LEID","CDR_NO"],
  "42_LE Main Tx.json": ["TAX_YEAR","LEID","CDR_NO"],
  "43_BSLA Master data.json": ["TAX_YEAR","BSLA_CODE"],
  "44_4562 Details View.json": ["TAX_YEAR","LEID","CDR_NO"],
  "45_M2 Details Data.json":["TAX_YEAR","LEID","CDR_NO"],
  "46_ScheduleC Details View.json":["TAX_YEAR","LEID","CDR_NO"],
  "47_PSHIP PRT Ratios.json":["TAX_YEAR","PSHIP_LEID","PSHIP_CDR_NO"],
  "48_EXT Partners Data.json":["TAX_YEAR","PTR_LEID","PTR_CDR_NO","PTR_REPORTING_PERIOD"],
  "49_IRS Form Data.json":["TAX_YEAR","LEID","CDR_NO"],
  "50_1065 Data.json":["TAX_YEAR","LEID"],
  "51_1065 Textual Info.json":["TAX_YEAR","IRS_FORM_NO","LEID","CDR_NO"],
  "64_TDIR LE BSLA Assign.json":["TAX_YEAR","LEID","CDR_NO"],
  "65_GTW Dividends Payor Details.json" : ["LEID","TAX_YEAR"]

}


producer_json = {
  "producer": "GTW",
  "schema": "StateTaxFiling"
}


# print(silver_tables_join_columns["50_1065 Data.json"])
# print(files_for_null_check["40_PDF Metadata Service_response.json"])
# print(PwCLabsSDK["Environment"])
# print(producer_json["producer"])