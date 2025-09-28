# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN
# MAGIC - Generated from Oracle Import file using IMPD_Convert_Map_Steps_Ora2DBX.py
# MAGIC - Export date: 2025-09-12 19:14:32

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

map_id = 'ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_owner.DIFF_ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN'

# Get the maximum UNK_KEY from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX(unk_key), 0) as max_key 
    FROM gap_catalog.ads_owner.unknown_table
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum UNK_KEY: {max_key}")

spark.sql(f"SET var.dif_table_name = {dif_table}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
p_process_key = dbutils.widgets.get("p_process_key")
print("p_load_date: "+p_load_date)
print("p_process_key: "+p_process_key)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_MAIN

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_MAIN 
# MAGIC      SELECT CPPROP_SOURCE_ID,
# MAGIC                CPPROP_SOURCE_SYSTEM_ID,
# MAGIC                CPPROP_SOURCE_SYS_ORIGIN,
# MAGIC                CPPROP_PARENT_SOURCE_ID,
# MAGIC                CPPTP_SOURCE_ID,
# MAGIC                CASETYPE,
# MAGIC                CPPROP_VALUE_TEXT,
# MAGIC                CPPROP_VALUE_NUMBER,
# MAGIC                CPPROP_VALUE_DATE,
# MAGIC                STRUCTURETYPE,
# MAGIC                MAPPING,
# MAGIC                CASE_KEY,
# MAGIC                CTP_KEY,
# MAGIC                CPPROP_KEY,
# MAGIC                ATTR_FP,
# MAGIC                CPPTP_KEY,
# MAGIC                CPPROP_PARENT_KEY
# MAGIC           FROM (WITH
# MAGIC                     STEP_0
# MAGIC                     AS
# MAGIC                     (select  /*+ materialize */ valid_to_date from
# MAGIC                         (   SELECT ADD_MONTHS (LAST_DAY ( to_date('20250911','yyyymmdd')), LEVEL - 1)    AS VALID_TO_DATE
# MAGIC                                FROM DUAL
# MAGIC                          CONNECT BY MONTHS_BETWEEN (
# MAGIC                                         LAST_DAY (TRUNC (CURRENT_TIMESTAMP())),
# MAGIC                                         ADD_MONTHS (LAST_DAY ( to_date('20250911','yyyymmdd')), LEVEL - 1)) >
# MAGIC                                     0
# MAGIC                          UNION
# MAGIC                          SELECT to_date('20250911','yyyymmdd') AS VALID_TO_DATE FROM DUAL
# MAGIC                          UNION ALL
# MAGIC                          SELECT DATE '3000-01-01' AS VALID_TO_DATE FROM DUAL)
# MAGIC                       ),
# MAGIC                     STEP_1
# MAGIC                     AS
# MAGIC                       (select  CASE_SOURCE_ID,
# MAGIC                                 CPPROP_SOURCE_ID,
# MAGIC                                 CPPROP_PARENT_SOURCE_ID,
# MAGIC                                 CPPTP_SOURCE_ID,
# MAGIC                                 CASETYPE,
# MAGIC                                 CTP_KEY,
# MAGIC                                 ATTR_TYPE,
# MAGIC                                 ATTR_BVALUE,
# MAGIC                                 ATTR_VALUE,
# MAGIC                                 STRUCTURETYPE,
# MAGIC                                 MAPPING,
# MAGIC                                 ATTR_FP,
# MAGIC                                 CASE
# MAGIC                                     WHEN SNAP.ATTR_TYPE = 'TEXT'
# MAGIC                                     THEN
# MAGIC                                         CAST (
# MAGIC                                             SUBSTR (SNAP.ATTR_BVALUE, 1, 4000)
# MAGIC                                                 AS varchar (4000))
# MAGIC                                     ELSE
# MAGIC                                         SUBSTR (SNAP.ATTR_VALUE, 1, 4000)
# MAGIC                                 END    AS CPPROP_VALUE_TEXT,
# MAGIC                                 CASE
# MAGIC                                     WHEN     SNAP.ATTR_TYPE IN
# MAGIC                                                  ('NUMBER', 'DECIMAL')
# MAGIC                                          AND TRIM (
# MAGIC                                                  CAST (
# MAGIC                                                      SUBSTR (SNAP.ATTR_VALUE,
# MAGIC                                                              1,
# MAGIC                                                              4000)
# MAGIC                                                          AS varchar (4000))) NOT IN
# MAGIC                                                  ('Infinity', 'NaN')
# MAGIC                                     THEN
# MAGIC                                         TO_NUMBER (
# MAGIC                                             REPLACE (TRIM (SNAP.ATTR_VALUE),
# MAGIC                                                      '.',
# MAGIC                                                      ','))
# MAGIC                                     ELSE
# MAGIC                                         CAST (NULL AS NUMBER)
# MAGIC                                 END    AS CPPROP_VALUE_NUMBER 
# MAGIC                             FROM gap_catalog.ads_etl_owner.STG_SMA_CASE_PHASE_PROPERTIES SNAP
# MAGIC                           WHERE    SNAP.ATTR_VALUE IS NOT NULL
# MAGIC                                 OR SNAP.ATTR_BVALUE IS NOT NULL
# MAGIC                                 OR SNAP.ATTR_TYPE IN ('MAP', 'LIST')
# MAGIC                      ),
# MAGIC                     STEP_2
# MAGIC                     AS
# MAGIC                         (SELECT /* no_merge */
# MAGIC                                 S1.CASE_SOURCE_ID,
# MAGIC                                 S1.CPPROP_SOURCE_ID,
# MAGIC                                 S1.CPPROP_PARENT_SOURCE_ID,
# MAGIC                                 S1.CPPTP_SOURCE_ID,
# MAGIC                                 S1.CASETYPE,
# MAGIC                                 S1.ATTR_TYPE,
# MAGIC                                 S1.ATTR_BVALUE,
# MAGIC                                 S1.ATTR_VALUE,
# MAGIC                                 S1.STRUCTURETYPE,
# MAGIC                                 S1.MAPPING,
# MAGIC                                 S1.ATTR_FP,
# MAGIC                                 S1.CPPROP_VALUE_TEXT,
# MAGIC                                 S1.CPPROP_VALUE_NUMBER,
# MAGIC                                 NVL (C.CASE_KEY, -1)     AS CASE_KEY,
# MAGIC                                 NVL (C.CTP_KEY, -1)      AS CTP_KEY,
# MAGIC                                 C.CASE_START_DATE
# MAGIC                            FROM STEP_1  S1, 
# MAGIC                               (select * from gap_catalog.ads_owner.CASES 
# MAGIC                                 where CASE_SOURCE_SYSTEM_ID = 'SMA' 
# MAGIC                                   and CASE_SOURCE_SYS_ORIGIN = 'SMA_MONITOR_EVENTS'
# MAGIC                                   and CASE_VALID_TO in (select valid_to_date from  STEP_0) 
# MAGIC                               ) C
# MAGIC                          where S1.CASE_SOURCE_ID = C.CASE_SOURCE_ID (+)
# MAGIC                             )
# MAGIC                 SELECT /*+ no_index(cpp) cardinality (CPP 200000000)*/
# MAGIC                        S2.CPPROP_SOURCE_ID            AS CPPROP_SOURCE_ID,
# MAGIC                        'SMA'                          AS CPPROP_SOURCE_SYSTEM_ID,
# MAGIC                        'SMA_MONITOR_EVENTS'           AS CPPROP_SOURCE_SYS_ORIGIN,
# MAGIC                        S2.CPPROP_PARENT_SOURCE_ID     AS CPPROP_PARENT_SOURCE_ID,
# MAGIC                        S2.CPPTP_SOURCE_ID             AS CPPTP_SOURCE_ID,
# MAGIC                        S2.CASETYPE                    AS CASETYPE,
# MAGIC                        S2.CPPROP_VALUE_TEXT           AS CPPROP_VALUE_TEXT,
# MAGIC                        S2.CPPROP_VALUE_NUMBER         AS CPPROP_VALUE_NUMBER,
# MAGIC                        CAST (NULL AS DATE)            AS CPPROP_VALUE_DATE,
# MAGIC                        S2.STRUCTURETYPE               AS STRUCTURETYPE,
# MAGIC                        S2.MAPPING                     AS MAPPING,
# MAGIC                        S2.CASE_KEY                    AS CASE_KEY,
# MAGIC                        S2.CTP_KEY                     AS CTP_KEY,
# MAGIC                        NVL (CPP.CPPROP_KEY, -1)       AS CPPROP_KEY,
# MAGIC                        S2.ATTR_FP                     AS ATTR_FP,
# MAGIC                        -1                             AS CPPTP_KEY,
# MAGIC                        -1                             AS CPPROP_PARENT_KEY
# MAGIC                   FROM STEP_2  S2
# MAGIC                        LEFT JOIN gap_catalog.ads_owner.CASE_PHASE_PROPERTIES CPP
# MAGIC                            ON     S2.CPPROP_SOURCE_ID = CPP.CPPROP_SOURCE_ID
# MAGIC                               AND S2.CASE_START_DATE = CPP.CASE_START_DATE
# MAGIC                               AND S2.CTP_KEY = CPP.CTP_KEY
# MAGIC                               AND CPP.CPPROP_SOURCE_SYS_ORIGIN =
# MAGIC                                   'SMA_MONITOR_EVENTS'
# MAGIC                               AND CPP.CPPROP_SOURCE_SYSTEM_ID = 'SMA'
# MAGIC                               AND CPP.CPPROP_VALID_TO = DATE '3000-01-01')     
# MAGIC ;
# MAGIC /*TRUNCATE*/
# MAGIC begin gap_catalog.ads_owner.utl_ddl.truncate_table
# MAGIC                      (p_owner => 'ADS_OWNER',
# MAGIC                       p_table => 'SMA_CASE_PHASE_PROPERTIES');
# MAGIC              end;;
# MAGIC /*INSERT*/
# MAGIC insert /*+ append */  into gap_catalog.ads_owner.SMA_CASE_PHASE_PROPERTIES trg 
# MAGIC  ( CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_SOURCE_ID, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CPPTP_SOURCE_ID, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CASETYPE, 
# MAGIC    CASE_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    STRUCTURETYPE, 
# MAGIC    MAPPING, 
# MAGIC    ATTR_FP, 
# MAGIC    SMCPP_INSERTED_DATETIME, 
# MAGIC    SMCPP_INSERT_PROCESS_KEY, 
# MAGIC    SMCPP_UPDATED_DATETIME, 
# MAGIC    SMCPP_UPDATE_PROCESS_KEY)
# MAGIC select /*+ full(src) */  CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_SOURCE_ID, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CPPTP_SOURCE_ID, 
# MAGIC    CPPTP_KEY, 
# MAGIC    CASETYPE, 
# MAGIC    CASE_KEY, 
# MAGIC    CTP_KEY, 
# MAGIC    CPPROP_VALUE_TEXT, 
# MAGIC    CPPROP_VALUE_NUMBER, 
# MAGIC    CPPROP_VALUE_DATE, 
# MAGIC    STRUCTURETYPE, 
# MAGIC    MAPPING, 
# MAGIC    ATTR_FP, 
# MAGIC    CURRENT_TIMESTAMP() as SMCPP_INSERTED_DATETIME, 
# MAGIC    13165092 as SMCPP_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as SMCPP_UPDATED_DATETIME, 
# MAGIC    13165092 as SMCPP_UPDATE_PROCESS_KEY
# MAGIC   from gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_MAIN src

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gap_catalog.ads_owner.DIFF_ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN;

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
# MAGIC CREATE TABLE gap_catalog.ads_owner.DIFF_ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN (
# MAGIC   tech_del_flg  CHAR(1),
# MAGIC   tech_new_rec  CHAR(1),
# MAGIC   tech_rid      VARCHAR(655),
# MAGIC   UNK_KEY  INTEGER,
# MAGIC   UNK_KEY_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${var.max_key} INCREMENT BY 1),
# MAGIC   UNK_SOURCE_ID  VARCHAR(120),
# MAGIC   UNK_SOURCE_SYSTEM_ID  VARCHAR(120),
# MAGIC   UNK_SOURCE_SYS_ORIGIN  VARCHAR(120),
# MAGIC   UNK_DESC  VARCHAR(4000)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
# MAGIC -- DIFF insert new section not found in source SQL

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
# MAGIC -- DIFF insert deleted section not found in source SQL

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
# MAGIC -- Target update section not found in source SQL

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
# MAGIC -- Target insert changed section not found in source SQL

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
# MAGIC -- Target insert new section not found in source SQL
# MAGIC
