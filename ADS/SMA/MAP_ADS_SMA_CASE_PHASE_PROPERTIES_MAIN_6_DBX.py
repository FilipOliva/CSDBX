# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6
# MAGIC - Generated from Oracle Import file
# MAGIC - Export date: 2025-09-13 17:13:24

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conversion notes
# MAGIC - Converted Oracle (+) join syntax to ANSI LEFT JOIN
# MAGIC - Replaced Oracle sequence with auto-increment column
# MAGIC - Updated schema names for Databricks catalog structure
# MAGIC - Converted YYYYMMDD format to ddMMyyyy
# MAGIC - Converted column names with $ to _
# MAGIC - Converted timezone handling for CET

# COMMAND ----------

# DBTITLE 1,Set Parameters

dbutils.widgets.text("p_load_date", "2025-08-31", "Load Date")
dbutils.widgets.text("p_process_key", "13165092", "Process Key")

map_id = 'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_owner.DIFF_ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6_ADS_MAP_SCD_DIFF'

# Get the maximum CPPROP_KEY from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX(cpprop_key), 0) as max_key 
    FROM gap_catalog.ads_owner.cpprop
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum CPPROP_KEY: {max_key}")

spark.sql(f"SET var.dif_table_name = {dif_table}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
print("p_load_date: "+p_load_date)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_SMA_CASE_PHASE_PROPERTIES_MAIN_6

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_SMA_CASE_PHASE_PROPERTIES_MAIN_6 
# MAGIC       WITH valid_case_types AS (
# MAGIC           SELECT CTP_KEY
# MAGIC           FROM gap_catalog.ads_owner.case_types
# MAGIC           WHERE CTP_CATEGORY = 'SMARTCASE'
# MAGIC             AND CTP_VALID_TO = TO_DATE('30000101', 'YYYYMMDD')
# MAGIC             and mod(ctp_key,10) = 6
# MAGIC       )
# MAGIC       SELECT
# MAGIC           STG_SMA.CPPROP_KEY,
# MAGIC           STG_SMA.CPPROP_SOURCE_ID,
# MAGIC           STG_SMA.CPPROP_SOURCE_SYSTEM_ID,
# MAGIC           STG_SMA.CPPROP_SOURCE_SYS_ORIGIN,
# MAGIC           STG_SMA.CPPROP_PARENT_KEY,
# MAGIC           -1 AS CASEPH_KEY,
# MAGIC           STG_SMA.CPPTP_KEY,
# MAGIC           NVL(CAS.CTP_KEY, -1) AS CTP_KEY,
# MAGIC           STG_SMA.CASE_KEY,
# MAGIC           NVL(CAS.CASE_START_DATE, TO_DATE('01011000', 'DDMMYYYY')) AS CASE_START_DATE,
# MAGIC           STG_SMA.CPPROP_VALUE_TEXT,
# MAGIC           STG_SMA.CPPROP_VALUE_DATE,
# MAGIC           STG_SMA.CPPROP_VALUE_NUMBER,
# MAGIC           -1 AS CPPRV_KEY
# MAGIC       FROM
# MAGIC           gap_catalog.ads_owner.SMA_CASE_PHASE_PROPERTIES STG_SMA
# MAGIC       LEFT JOIN
# MAGIC           gap_catalog.ads_owner.CASES PARTITION(PARTITION_30000101) CAS
# MAGIC       ON
# MAGIC           CAS.CASE_KEY = STG_SMA.CASE_KEY
# MAGIC           AND CAS.CTP_KEY IN (SELECT CTP_KEY FROM valid_case_types)
# MAGIC       WHERE
# MAGIC           STG_SMA.CTP_KEY IN (SELECT CTP_KEY FROM valid_case_types)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gap_catalog.ads_owner.DIFF_ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6_ADS_MAP_SCD_DIFF;

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
# MAGIC create  table gap_catalog.ads_owner.DIFF_ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6_ADS_MAP_SCD_DIFF
# MAGIC ( tech_del_flg  char(1),
# MAGIC   tech_new_rec  char(1),
# MAGIC   tech_rid      varchar(255),
# MAGIC   CPPROP_KEY  INTEGER,
# MAGIC   CPPROP_KEY_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${var.max_key} INCREMENT BY 1),
# MAGIC   CPPROP_SOURCE_ID  varchar(400),
# MAGIC   CPPROP_SOURCE_SYSTEM_ID  varchar(120),
# MAGIC   CPPROP_SOURCE_SYS_ORIGIN  varchar(120),
# MAGIC   CPPROP_PARENT_KEY  INTEGER,
# MAGIC   CASEPH_KEY  INTEGER,
# MAGIC   CPPTP_KEY  INTEGER,
# MAGIC   CTP_KEY  INTEGER,
# MAGIC   CASE_KEY  INTEGER,
# MAGIC   CASE_START_DATE  DATE,
# MAGIC   CPPROP_VALUE_TEXT  varchar(4000),
# MAGIC   CPPROP_VALUE_DATE  DATE,
# MAGIC   CPPROP_VALUE_NUMBER  NUMBER,
# MAGIC   CPPRV_KEY  INTEGER)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   CPPROP_KEY,
# MAGIC   CPPROP_SOURCE_ID,
# MAGIC   CPPROP_SOURCE_SYSTEM_ID,
# MAGIC   CPPROP_SOURCE_SYS_ORIGIN,
# MAGIC   CPPROP_PARENT_KEY,
# MAGIC   CASEPH_KEY,
# MAGIC   CPPTP_KEY,
# MAGIC   CTP_KEY,
# MAGIC   CASE_KEY,
# MAGIC   CASE_START_DATE,
# MAGIC   CPPROP_VALUE_TEXT,
# MAGIC   CPPROP_VALUE_DATE,
# MAGIC   CPPROP_VALUE_NUMBER,
# MAGIC   CPPRV_KEY
# MAGIC )
# MAGIC select   'N' as tech_del_flg, 
# MAGIC     case when trg.CPPROP_KEY is null then 'Y' else 'N' end as tech_new_rec,
# MAGIC     trg.rid as tech_rid,
# MAGIC    src.CPPROP_KEY, 
# MAGIC    src.CPPROP_SOURCE_ID, 
# MAGIC    src.CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    src.CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    src.CPPROP_PARENT_KEY, 
# MAGIC    src.CASEPH_KEY, 
# MAGIC    src.CPPTP_KEY, 
# MAGIC    src.CTP_KEY, 
# MAGIC    src.CASE_KEY, 
# MAGIC    src.CASE_START_DATE, 
# MAGIC    src.CPPROP_VALUE_TEXT, 
# MAGIC    src.CPPROP_VALUE_DATE, 
# MAGIC    src.CPPROP_VALUE_NUMBER, 
# MAGIC    src.CPPRV_KEY
# MAGIC  from 
# MAGIC     (select /*+ full(xc) */      CPPROP_KEY, 
# MAGIC        CPPROP_SOURCE_ID, 
# MAGIC        CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC        CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC        CPPROP_PARENT_KEY, 
# MAGIC        CASEPH_KEY, 
# MAGIC        CPPTP_KEY, 
# MAGIC        CTP_KEY, 
# MAGIC        CASE_KEY, 
# MAGIC        CASE_START_DATE, 
# MAGIC        CPPROP_VALUE_TEXT, 
# MAGIC        CPPROP_VALUE_DATE, 
# MAGIC        CPPROP_VALUE_NUMBER, 
# MAGIC        CPPRV_KEY
# MAGIC        from gap_catalog.ads_etl_owner.XC_SMA_CASE_PHASE_PROPERTIES_MAIN_6 xc
# MAGIC     where (CPPROP_SOURCE_SYSTEM_ID = 'SMA'
# MAGIC             AND CPPROP_SOURCE_SYS_ORIGIN = 'SMA_MONITOR_EVENTS'
# MAGIC             AND CTP_KEY IN ( 22706,7316,17726,156,176,206,266,1486,70176,3806,1476,9576,13586,58776,566,6,26,6736,12406,1466,9546,616,50876,47876,13646,18276,1286,1296,1426,1306,1076,1496,1526,546,1416,1446,1456,986,1056,1196,736,9506,2396,2506,3606,3706,1246,48456,7306,2046,246,2376,3406,6576,416,81776,79326,65176,56976,76076,84806,84066,84936,84726,87796,89746,87256,85986,1136,87906,82756,20606,90186,63076,89756,92796,90636,92026,90316,92786,63376 ))) src LEFT JOIN
# MAGIC     (select  cpprop_key||'.'||cpprop_valid_from||'.'||ctp_key||'.'||case_start_date as rid, t.* from gap_catalog.ads_owner.CASE_PHASE_PROPERTIES t
# MAGIC       where CPPROP_CURRENT_FLAG  = 'Y'
# MAGIC         and CPPROP_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (CPPROP_SOURCE_SYSTEM_ID = 'SMA'
# MAGIC             AND CPPROP_SOURCE_SYS_ORIGIN = 'SMA_MONITOR_EVENTS'
# MAGIC             AND CTP_KEY IN ( 22706,7316,17726,156,176,206,266,1486,70176,3806,1476,9576,13586,58776,566,6,26,6736,12406,1466,9546,616,50876,47876,13646,18276,1286,1296,1426,1306,1076,1496,1526,546,1416,1446,1456,986,1056,1196,736,9506,2396,2506,3606,3706,1246,48456,7306,2046,246,2376,3406,6576,416,81776,79326,65176,56976,76076,84806,84066,84936,84726,87796,89746,87256,85986,1136,87906,82756,20606,90186,63076,89756,92796,90636,92026,90316,92786,63376 ))      ) trg
# MAGIC ON trg.CPPROP_KEY = src.CPPROP_KEY
# MAGIC  and trg.CPPROP_VALID_TO = to_date('30000101','yyyyMMdd') WHERE (
# MAGIC      decode( src.CPPROP_SOURCE_ID,trg.CPPROP_SOURCE_ID,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_SOURCE_SYSTEM_ID,trg.CPPROP_SOURCE_SYSTEM_ID,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_SOURCE_SYS_ORIGIN,trg.CPPROP_SOURCE_SYS_ORIGIN,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_PARENT_KEY,trg.CPPROP_PARENT_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CASEPH_KEY,trg.CASEPH_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CPPTP_KEY,trg.CPPTP_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CTP_KEY,trg.CTP_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CASE_KEY,trg.CASE_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CASE_START_DATE,trg.CASE_START_DATE,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_VALUE_TEXT,trg.CPPROP_VALUE_TEXT,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_VALUE_DATE,trg.CPPROP_VALUE_DATE,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_VALUE_NUMBER,trg.CPPROP_VALUE_NUMBER,1,0 ) = 0  or
# MAGIC      decode( src.CPPRV_KEY,trg.CPPRV_KEY,1,0 ) = 0 or 
# MAGIC      trg.CPPROP_KEY is null or 
# MAGIC      trg.CPPROP_DELETED_FLAG = 'Y'
# MAGIC    );

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
# MAGIC -- DIFF insert deleted section not found

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
# MAGIC update  gap_catalog.ads_owner.CASE_PHASE_PROPERTIES set
# MAGIC        CPPROP_UPDATED_DATETIME = CURRENT_TIMESTAMP(),
# MAGIC        CPPROP_UPDATE_PROCESS_KEY = 13165092,
# MAGIC        CPPROP_CURRENT_FLAG = 'N', 
# MAGIC        CPPROP_VALID_TO = to_date('20250911','YYYYMMDD')-1
# MAGIC  where CPPROP_CURRENT_FLAG = 'Y'
# MAGIC    and CPPROP_VALID_TO = to_date('30000101','yyyyMMdd')
# MAGIC    and cpprop_key||'.'||cpprop_valid_from||'.'||ctp_key||'.'||case_start_date in (select tech_rid from ${var.dif_table_name} where tech_rid is not null);

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.CASE_PHASE_PROPERTIES 
# MAGIC  ( CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CASEPH_KEY, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CASE_KEY, 
# MAGIC    CASE_START_DATE, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    CPPROP_VALID_FROM, 
# MAGIC    to_date('30000101','yyyyMMdd') as CPPROP_VALID_TO
# MAGIC    CPPROP_CURRENT_FLAG, 
# MAGIC    CPPROP_DELETED_FLAG, 
# MAGIC    CPPROP_INSERTED_DATETIME, 
# MAGIC    CPPROP_INSERT_PROCESS_KEY, 
# MAGIC    CPPROP_UPDATED_DATETIME, 
# MAGIC    CPPROP_UPDATE_PROCESS_KEY, 
# MAGIC    CPPRV_KEY)
# MAGIC select CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CASEPH_KEY, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CASE_KEY, 
# MAGIC    CASE_START_DATE, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as CPPROP_VALID_FROM, 
# MAGIC    to_date('30000101','yyyyMMdd') as to_date('30000101','yyyyMMdd') as CPPROP_VALID_TO
# MAGIC    'Y' as CPPROP_CURRENT_FLAG, 
# MAGIC    tech_del_flg as CPPROP_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as CPPROP_INSERTED_DATETIME, 
# MAGIC    $p_process_key as CPPROP_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as CPPROP_UPDATED_DATETIME, 
# MAGIC    $p_process_key as CPPROP_UPDATE_PROCESS_KEY, 
# MAGIC    CPPRV_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'N';

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.CASE_PHASE_PROPERTIES 
# MAGIC  ( CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CASEPH_KEY, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CASE_KEY, 
# MAGIC    CASE_START_DATE, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    CPPROP_VALID_FROM, 
# MAGIC    to_date('30000101','yyyyMMdd') as CPPROP_VALID_TO
# MAGIC    CPPROP_CURRENT_FLAG, 
# MAGIC    CPPROP_DELETED_FLAG, 
# MAGIC    CPPROP_INSERTED_DATETIME, 
# MAGIC    CPPROP_INSERT_PROCESS_KEY, 
# MAGIC    CPPROP_UPDATED_DATETIME, 
# MAGIC    CPPROP_UPDATE_PROCESS_KEY, 
# MAGIC    CPPRV_KEY)
# MAGIC select CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CASEPH_KEY, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CASE_KEY, 
# MAGIC    CASE_START_DATE, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as CPPROP_VALID_FROM, 
# MAGIC    to_date('30000101','yyyyMMdd') as to_date('30000101','yyyyMMdd') as CPPROP_VALID_TO
# MAGIC    'Y' as CPPROP_CURRENT_FLAG, 
# MAGIC    tech_del_flg as CPPROP_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as CPPROP_INSERTED_DATETIME, 
# MAGIC    $p_process_key as CPPROP_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as CPPROP_UPDATED_DATETIME, 
# MAGIC    $p_process_key as CPPROP_UPDATE_PROCESS_KEY, 
# MAGIC    CPPRV_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'Y';

# COMMAND ----------

# DBTITLE 1,Validation - Row Counts
# MAGIC %sql 
# MAGIC select * from (
# MAGIC select '1-Source_Table', count(1) rec_cnt from gap_catalog.ads_etl_owner.DLK_ADS_LOV_RDS_ANALYTICALEVENTSTATUS where sys = 'Brasil'
# MAGIC union all
# MAGIC select '2-XC_SMA_CASE_PHASE_PROPERTIES_MAIN_6', count(1) from gap_catalog.ads_etl_owner.XC_SMA_CASE_PHASE_PROPERTIES_MAIN_6
# MAGIC union all
# MAGIC select '3-DIFF_TABLE', count(1) from  ${var.dif_table_name}
# MAGIC union all
# MAGIC select '4-cpprop', count(1) from gap_catalog.ads_owner.cpprop where cpprop_source_sys_origin = 'RDS_ANALYTICALEVENTSTATUS'
# MAGIC ) order by 1
