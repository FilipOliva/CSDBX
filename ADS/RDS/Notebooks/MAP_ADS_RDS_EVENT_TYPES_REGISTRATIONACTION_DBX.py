# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_RDS_EVENT_TYPES_REGISTRATIONACTION
# MAGIC - Generated from Oracle Import file using IMPD_Convert_Map_Steps_Ora2DBX.py
# MAGIC - Export date: 2025-09-16 12:41:54

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

dbutils.widgets.text("p_load_date", "2025-08-14", "Load Date")
dbutils.widgets.text("p_process_key", "13173833", "Process Key")

map_id = 'ADS_RDS_EVENT_TYPES_REGISTRATIONACTION'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_TYPES_REGISTRATIONACTION_ADS_MAP_SCD_DIFF'

# Get the maximum EVETP_KEY from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX(evetp_key), 0) as max_key 
    FROM gap_catalog.ads_owner.event_types
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum EVETP_KEY: {max_key}")

spark.sql(f"SET var.dif_table_name = {dif_table}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
p_process_key = dbutils.widgets.get("p_process_key")
print("p_load_date: "+p_load_date)
print("p_process_key: "+p_process_key)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_RDS_EVENT_TYPES_REGISTRATIONACTION

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_RDS_EVENT_TYPES_REGISTRATIONACTION 
# MAGIC       with C_0FILTERAJDNR1FIIIND9JFP621UP020CF as (
# MAGIC       select
# MAGIC           RDS_IDENTITYREGISTRATIONACTION.value  VALUE
# MAGIC         from gap_catalog.ads_etl_owner.DLK_ADS_LOV_RDS_IDENTITYREGISTRATIONACTION RDS_IDENTITYREGISTRATIONACTION
# MAGIC         where ( RDS_IDENTITYREGISTRATIONACTION.sys   = 'Brasil'
# MAGIC             and RDS_IDENTITYREGISTRATIONACTION.lang  = 'EN'
# MAGIC             and CAST(from_utc_timestamp(SYS_EFFECTIVE_DATE, 'Europe/Prague') AS DATE) = '$p_load_date'
# MAGIC               )
# MAGIC       )
# MAGIC       select  /*+no hint*/
# MAGIC               /*seznam vkladanych nebo updatovanych sloupcu bez SK, auditnich atribudu a deleted flagu*/
# MAGIC           FILTER_A.VALUE as EVETP_SOURCE_ID,
# MAGIC           'RDS' as EVETP_SOURCE_SYSTEM_ID,
# MAGIC           'RDS_IDENTITYREGISTRATIONACTION' as EVETP_SOURCE_SYS_ORIGIN,
# MAGIC           FILTER_A.VALUE as EVETP_DESC,
# MAGIC           'PROCESS_EVENTS' as EVETP_EVENT_TABLE_NAME,
# MAGIC           'XNA' as EVETP_TARGET,
# MAGIC           'XNA' as EVETP_DATA_PATH,
# MAGIC           'N' as EVETP_DELETED_FLAG
# MAGIC         from (C_0FILTERAJDNR1FIIIND9JFP621UP020CF FILTER_A)
# MAGIC         where (1=1)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_TYPES_REGISTRATIONACTION_ADS_MAP_SCD_DIFF;

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
# MAGIC create  table gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_TYPES_REGISTRATIONACTION_ADS_MAP_SCD_DIFF
# MAGIC ( tech_del_flg  char(1),
# MAGIC   tech_new_rec  char(1),
# MAGIC   tech_rid      varchar(255),
# MAGIC   EVETP_KEY  INTEGER,
# MAGIC   EVETP_KEY_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${var.max_key} INCREMENT BY 1),
# MAGIC   EVETP_SOURCE_ID  varchar(400),
# MAGIC   EVETP_SOURCE_SYSTEM_ID  varchar(120),
# MAGIC   EVETP_SOURCE_SYS_ORIGIN  varchar(120),
# MAGIC   EVETP_DESC  varchar(4000),
# MAGIC   EVETP_EVENT_TABLE_NAME  varchar(120),
# MAGIC   EVETP_TARGET  varchar(120),
# MAGIC   EVETP_DATA_PATH  varchar(120))
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   EVETP_KEY,
# MAGIC   EVETP_SOURCE_ID,
# MAGIC   EVETP_SOURCE_SYSTEM_ID,
# MAGIC   EVETP_SOURCE_SYS_ORIGIN,
# MAGIC   EVETP_DESC,
# MAGIC   EVETP_EVENT_TABLE_NAME,
# MAGIC   EVETP_TARGET,
# MAGIC   EVETP_DATA_PATH
# MAGIC )
# MAGIC select   'N' as tech_del_flg, 
# MAGIC     case when trg.EVETP_SOURCE_ID is null then 'Y' else 'N' end as tech_new_rec,
# MAGIC     trg.rid as tech_rid,
# MAGIC    trg.EVETP_KEY, 
# MAGIC    src.EVETP_SOURCE_ID, 
# MAGIC    src.EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    src.EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    src.EVETP_DESC, 
# MAGIC    src.EVETP_EVENT_TABLE_NAME, 
# MAGIC    src.EVETP_TARGET, 
# MAGIC    src.EVETP_DATA_PATH
# MAGIC  from 
# MAGIC     (select /*+ full(xc) */      EVETP_SOURCE_ID, 
# MAGIC        EVETP_SOURCE_SYSTEM_ID, 
# MAGIC        EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC        EVETP_DESC, 
# MAGIC        EVETP_EVENT_TABLE_NAME, 
# MAGIC        EVETP_DELETED_FLAG, 
# MAGIC        EVETP_TARGET, 
# MAGIC        EVETP_DATA_PATH
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_EVENT_TYPES_REGISTRATIONACTION xc
# MAGIC     where (EVETP_SOURCE_SYS_ORIGIN='RDS_IDENTITYREGISTRATIONACTION')) src LEFT JOIN
# MAGIC     (select  evetp_key||'.'||evetp_valid_from as rid, t.* from gap_catalog.ads_owner.EVENT_TYPES t
# MAGIC       where EVETP_CURRENT_FLAG  = 'Y'
# MAGIC         and EVETP_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (EVETP_SOURCE_SYS_ORIGIN='RDS_IDENTITYREGISTRATIONACTION')      ) trg
# MAGIC ON trg.EVETP_SOURCE_ID = src.EVETP_SOURCE_ID
# MAGIC  and trg.EVETP_SOURCE_SYSTEM_ID = src.EVETP_SOURCE_SYSTEM_ID
# MAGIC  and trg.EVETP_SOURCE_SYS_ORIGIN = src.EVETP_SOURCE_SYS_ORIGIN
# MAGIC  and trg.EVETP_VALID_TO = to_date('30000101','yyyyMMdd') WHERE (
# MAGIC      decode( src.EVETP_DESC,trg.EVETP_DESC,1,0 ) = 0  or
# MAGIC      decode( src.EVETP_EVENT_TABLE_NAME,trg.EVETP_EVENT_TABLE_NAME,1,0 ) = 0  or
# MAGIC      decode( src.EVETP_DELETED_FLAG,trg.EVETP_DELETED_FLAG,1,0 ) = 0  or
# MAGIC      decode( src.EVETP_TARGET,trg.EVETP_TARGET,1,0 ) = 0  or
# MAGIC      decode( src.EVETP_DATA_PATH,trg.EVETP_DATA_PATH,1,0 ) = 0 or 
# MAGIC      trg.EVETP_SOURCE_ID is null   );

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   EVETP_KEY,
# MAGIC   EVETP_SOURCE_ID,
# MAGIC   EVETP_SOURCE_SYSTEM_ID,
# MAGIC   EVETP_SOURCE_SYS_ORIGIN,
# MAGIC   EVETP_DESC,
# MAGIC   EVETP_EVENT_TABLE_NAME,
# MAGIC   EVETP_TARGET,
# MAGIC   EVETP_DATA_PATH
# MAGIC )
# MAGIC select   'Y' as tech_del_flg, 
# MAGIC  'N' as tech_new_rec, 
# MAGIC  trg.rid as tech_rid, 
# MAGIC    trg.EVETP_KEY, 
# MAGIC    trg.EVETP_SOURCE_ID, 
# MAGIC    trg.EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    trg.EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    trg.EVETP_DESC, 
# MAGIC    trg.EVETP_EVENT_TABLE_NAME, 
# MAGIC    trg.EVETP_TARGET, 
# MAGIC    trg.EVETP_DATA_PATH
# MAGIC  from 
# MAGIC     (select  evetp_key||'.'||evetp_valid_from as rid, t.* from gap_catalog.ads_owner.EVENT_TYPES t
# MAGIC       where EVETP_CURRENT_FLAG  = 'Y' and EVETP_DELETED_FLAG  = 'N'
# MAGIC         and EVETP_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (EVETP_SOURCE_SYS_ORIGIN='RDS_IDENTITYREGISTRATIONACTION')      ) trg LEFT JOIN
# MAGIC     (select /*+ full(xc) */      EVETP_SOURCE_ID, 
# MAGIC        EVETP_SOURCE_SYSTEM_ID, 
# MAGIC        EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC        to_date('30000101','yyyyMMdd') as EVETP_VALID_TO
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_EVENT_TYPES_REGISTRATIONACTION xc
# MAGIC     where (EVETP_SOURCE_SYS_ORIGIN='RDS_IDENTITYREGISTRATIONACTION')) src
# MAGIC ON trg.EVETP_SOURCE_ID = src.EVETP_SOURCE_ID
# MAGIC  and trg.EVETP_SOURCE_SYSTEM_ID = src.EVETP_SOURCE_SYSTEM_ID
# MAGIC  and trg.EVETP_SOURCE_SYS_ORIGIN = src.EVETP_SOURCE_SYS_ORIGIN
# MAGIC  and trg.EVETP_VALID_TO = src.EVETP_VALID_TO WHERE (src.EVETP_SOURCE_ID is null);

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
# MAGIC update  gap_catalog.ads_owner.EVENT_TYPES set
# MAGIC        EVETP_UPDATED_DATETIME = CURRENT_TIMESTAMP(),
# MAGIC        EVETP_UPDATE_PROCESS_KEY = 13173833,
# MAGIC        EVETP_CURRENT_FLAG = 'N', 
# MAGIC        EVETP_VALID_TO = to_date('$p_load_date','yyyy-MM-dd')-1
# MAGIC  where EVETP_CURRENT_FLAG = 'Y'
# MAGIC    and EVETP_VALID_TO = to_date('30000101','yyyyMMdd')
# MAGIC    and evetp_key||'.'||evetp_valid_from in (select tech_rid from ${var.dif_table_name} where tech_rid is not null);

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.EVENT_TYPES 
# MAGIC  ( EVETP_KEY, 
# MAGIC    EVETP_SOURCE_ID, 
# MAGIC    EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    EVETP_DESC, 
# MAGIC    EVETP_EVENT_TABLE_NAME, 
# MAGIC    EVETP_VALID_FROM, 
# MAGIC    EVETP_VALID_TO, 
# MAGIC    EVETP_CURRENT_FLAG, 
# MAGIC    EVETP_DELETED_FLAG, 
# MAGIC    EVETP_INSERTED_DATETIME, 
# MAGIC    EVETP_INSERT_PROCESS_KEY, 
# MAGIC    EVETP_UPDATED_DATETIME, 
# MAGIC    EVETP_UPDATE_PROCESS_KEY, 
# MAGIC    EVETP_TARGET, 
# MAGIC    EVETP_DATA_PATH)
# MAGIC select EVETP_KEY, 
# MAGIC    EVETP_SOURCE_ID, 
# MAGIC    EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    EVETP_DESC, 
# MAGIC    EVETP_EVENT_TABLE_NAME, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as EVETP_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as EVETP_VALID_TO, 
# MAGIC    'Y' as EVETP_CURRENT_FLAG, 
# MAGIC    tech_del_flg as EVETP_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as EVETP_INSERTED_DATETIME, 
# MAGIC    $p_process_key as EVETP_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as EVETP_UPDATED_DATETIME, 
# MAGIC    $p_process_key as EVETP_UPDATE_PROCESS_KEY, 
# MAGIC    EVETP_TARGET, 
# MAGIC    EVETP_DATA_PATH
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'N';

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.EVENT_TYPES 
# MAGIC  ( EVETP_KEY, 
# MAGIC    EVETP_SOURCE_ID, 
# MAGIC    EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    EVETP_DESC, 
# MAGIC    EVETP_EVENT_TABLE_NAME, 
# MAGIC    EVETP_VALID_FROM, 
# MAGIC    EVETP_VALID_TO, 
# MAGIC    EVETP_CURRENT_FLAG, 
# MAGIC    EVETP_DELETED_FLAG, 
# MAGIC    EVETP_INSERTED_DATETIME, 
# MAGIC    EVETP_INSERT_PROCESS_KEY, 
# MAGIC    EVETP_UPDATED_DATETIME, 
# MAGIC    EVETP_UPDATE_PROCESS_KEY, 
# MAGIC    EVETP_TARGET, 
# MAGIC    EVETP_DATA_PATH)
# MAGIC select EVETP_KEY_NEW as EVETP_KEY, 
# MAGIC    EVETP_SOURCE_ID, 
# MAGIC    EVETP_SOURCE_SYSTEM_ID, 
# MAGIC    EVETP_SOURCE_SYS_ORIGIN, 
# MAGIC    EVETP_DESC, 
# MAGIC    EVETP_EVENT_TABLE_NAME, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as EVETP_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as EVETP_VALID_TO, 
# MAGIC    'Y' as EVETP_CURRENT_FLAG, 
# MAGIC    tech_del_flg as EVETP_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as EVETP_INSERTED_DATETIME, 
# MAGIC    $p_process_key as EVETP_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as EVETP_UPDATED_DATETIME, 
# MAGIC    $p_process_key as EVETP_UPDATE_PROCESS_KEY, 
# MAGIC    EVETP_TARGET, 
# MAGIC    EVETP_DATA_PATH
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'Y';
# MAGIC
