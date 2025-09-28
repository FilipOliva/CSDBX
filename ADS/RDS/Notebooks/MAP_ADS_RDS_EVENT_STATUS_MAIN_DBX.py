# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_RDS_EVENT_STATUS_MAIN
# MAGIC - Generated from Oracle Import file using IMPD_Convert_Map_Steps_Ora2DBX.py
# MAGIC - Export date: 2025-09-16 12:41:50

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

map_id = 'ADS_RDS_EVENT_STATUS_MAIN'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_STATUS_MAIN_ADS_MAP_SCD_DIFF'

# Get the maximum EST_KEY from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX(est_key), 0) as max_key 
    FROM gap_catalog.ads_owner.event_status
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum EST_KEY: {max_key}")

spark.sql(f"SET var.dif_table_name = {dif_table}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
p_process_key = dbutils.widgets.get("p_process_key")
print("p_load_date: "+p_load_date)
print("p_process_key: "+p_process_key)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_RDS_EVENT_STATUS_MAIN

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_RDS_EVENT_STATUS_MAIN 
# MAGIC       with C_0FILTERAOT65KS2DJGI939C7EA9B364HJ as (
# MAGIC       select
# MAGIC           RDS_SENDNOTIFICATIONSTATUS_1.id  ID,
# MAGIC           RDS_SENDNOTIFICATIONSTATUS_1.value  VALUE
# MAGIC         from gap_catalog.ads_etl_owner.DLK_ADS_LOV_RDS_SENDNOTIFICATIONSTATUS RDS_SENDNOTIFICATIONSTATUS_1
# MAGIC         where ( CAST(from_utc_timestamp(SYS_EFFECTIVE_DATE, 'Europe/Prague') AS DATE) = '$p_load_date'
# MAGIC             and RDS_SENDNOTIFICATIONSTATUS_1.sys = 'Brasil'
# MAGIC             and RDS_SENDNOTIFICATIONSTATUS_1.lang = 'CZ'
# MAGIC               )
# MAGIC       )
# MAGIC       select 	/*+no hint*/
# MAGIC               /*seznam vkladanych nebo updatovanych sloupcu bez SK, auditnich atribudu a deleted flagu*/
# MAGIC           SET_1.ID as EST_SOURCE_ID,
# MAGIC           SET_1.SOURCE_SYSTEM_ID as EST_SOURCE_SYSTEM_ID,
# MAGIC           SET_1.ORIGIN as EST_SOURCE_SYS_ORIGIN,
# MAGIC           nvl(SET_1.VALUE, 'XNA') as EST_DESC,
# MAGIC           'N' as EST_DELETED_FLAG
# MAGIC         from
# MAGIC         ( ( select
# MAGIC                 FILTER_A.ID as ID,
# MAGIC                 FILTER_A.VALUE as VALUE,
# MAGIC                 'WBL' as SOURCE_SYSTEM_ID,
# MAGIC                 'RDS_SENDNOTIFICATIONSTATUS' as ORIGIN
# MAGIC               from C_0FILTERAOT65KS2DJGI939C7EA9B364HJ FILTER_A
# MAGIC             UNION
# MAGIC             select
# MAGIC                 DISTINCT_.SOURCE_ID_PH1 as ID,
# MAGIC                 DISTINCT_.DESC_PH1 as VALUE,
# MAGIC                 DISTINCT_.SOURCE_SYSTEM_ID_PH1 as SOURCE_SYSTEM_ID,
# MAGIC                 DISTINCT_.ORIGIN_PH1 as ORIGIN
# MAGIC               from
# MAGIC               ( select
# MAGIC                     distinct
# MAGIC                     'XER' as SOURCE_ID_PH1,
# MAGIC                     'XNA' as SOURCE_SYSTEM_ID_PH1,
# MAGIC                     'INVALID' as DESC_PH1,
# MAGIC                     'XNA' as ORIGIN_PH1
# MAGIC                ) DISTINCT_
# MAGIC             UNION
# MAGIC             select
# MAGIC                 DISTINCT_1.SOURCE_ID_PH2 as ID,
# MAGIC                 DISTINCT_1.DESC_PH2 as VALUE,
# MAGIC                 DISTINCT_1.SOURCE_SYSTEM_ID_PH2 as SOURCE_SYSTEM_ID,
# MAGIC                 DISTINCT_1.ORIGIN_PH2 as ORIGIN
# MAGIC               from
# MAGIC               ( select
# MAGIC                     distinct
# MAGIC                     'XNA' as SOURCE_ID_PH2,
# MAGIC                     'XNA' as SOURCE_SYSTEM_ID_PH2,
# MAGIC                     'N/A' as DESC_PH2,
# MAGIC                     'XNA' as ORIGIN_PH2
# MAGIC                ) DISTINCT_1
# MAGIC           ) SET_1
# MAGIC         )
# MAGIC         where	(1=1)
# MAGIC           and (1=1)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_STATUS_MAIN_ADS_MAP_SCD_DIFF;

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
# MAGIC create  table gap_catalog.ads_etl_owner.DIFF_ADS_RDS_EVENT_STATUS_MAIN_ADS_MAP_SCD_DIFF
# MAGIC ( tech_del_flg  char(1),
# MAGIC   tech_new_rec  char(1),
# MAGIC   tech_rid      varchar(255),
# MAGIC   EST_KEY  INTEGER,
# MAGIC   EST_KEY_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${var.max_key} INCREMENT BY 1),
# MAGIC   EST_SOURCE_ID  varchar(120),
# MAGIC   EST_SOURCE_SYSTEM_ID  varchar(120),
# MAGIC   EST_SOURCE_SYS_ORIGIN  varchar(120),
# MAGIC   EST_DESC  varchar(400))
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   EST_KEY,
# MAGIC   EST_SOURCE_ID,
# MAGIC   EST_SOURCE_SYSTEM_ID,
# MAGIC   EST_SOURCE_SYS_ORIGIN,
# MAGIC   EST_DESC
# MAGIC )
# MAGIC select   'N' as tech_del_flg, 
# MAGIC     case when trg.EST_SOURCE_ID is null then 'Y' else 'N' end as tech_new_rec,
# MAGIC     trg.rid as tech_rid,
# MAGIC    trg.EST_KEY, 
# MAGIC    src.EST_SOURCE_ID, 
# MAGIC    src.EST_SOURCE_SYSTEM_ID, 
# MAGIC    src.EST_SOURCE_SYS_ORIGIN, 
# MAGIC    src.EST_DESC
# MAGIC  from 
# MAGIC     (select /*+ full(xc) */      EST_SOURCE_ID, 
# MAGIC        EST_SOURCE_SYSTEM_ID, 
# MAGIC        EST_SOURCE_SYS_ORIGIN, 
# MAGIC        EST_DESC, 
# MAGIC        EST_DELETED_FLAG
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_EVENT_STATUS_MAIN xc
# MAGIC     where (EST_SOURCE_SYSTEM_ID in('WBL', 'XNA') and EST_SOURCE_SYS_ORIGIN in ('RDS_SENDNOTIFICATIONSTATUS','XNA'))) src LEFT JOIN
# MAGIC     (select  est_key||'.'||est_valid_from as rid, t.* from gap_catalog.ads_owner.EVENT_STATUS t
# MAGIC       where EST_CURRENT_FLAG  = 'Y'
# MAGIC         and EST_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (EST_SOURCE_SYSTEM_ID in('WBL', 'XNA') and EST_SOURCE_SYS_ORIGIN in ('RDS_SENDNOTIFICATIONSTATUS','XNA'))      ) trg
# MAGIC ON trg.EST_SOURCE_ID = src.EST_SOURCE_ID
# MAGIC  and trg.EST_SOURCE_SYSTEM_ID = src.EST_SOURCE_SYSTEM_ID
# MAGIC  and trg.EST_SOURCE_SYS_ORIGIN = src.EST_SOURCE_SYS_ORIGIN
# MAGIC  and trg.EST_VALID_TO = to_date('30000101','yyyyMMdd') WHERE (
# MAGIC      decode( src.EST_DESC,trg.EST_DESC,1,0 ) = 0  or
# MAGIC      decode( src.EST_DELETED_FLAG,trg.EST_DELETED_FLAG,1,0 ) = 0 or 
# MAGIC      trg.EST_SOURCE_ID is null   );

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   EST_KEY,
# MAGIC   EST_SOURCE_ID,
# MAGIC   EST_SOURCE_SYSTEM_ID,
# MAGIC   EST_SOURCE_SYS_ORIGIN,
# MAGIC   EST_DESC
# MAGIC )
# MAGIC select   'Y' as tech_del_flg, 
# MAGIC  'N' as tech_new_rec, 
# MAGIC  trg.rid as tech_rid, 
# MAGIC    trg.EST_KEY, 
# MAGIC    trg.EST_SOURCE_ID, 
# MAGIC    trg.EST_SOURCE_SYSTEM_ID, 
# MAGIC    trg.EST_SOURCE_SYS_ORIGIN, 
# MAGIC    trg.EST_DESC
# MAGIC  from 
# MAGIC     (select  est_key||'.'||est_valid_from as rid, t.* from gap_catalog.ads_owner.EVENT_STATUS t
# MAGIC       where EST_CURRENT_FLAG  = 'Y' and EST_DELETED_FLAG  = 'N'
# MAGIC         and EST_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (EST_SOURCE_SYSTEM_ID in('WBL', 'XNA') and EST_SOURCE_SYS_ORIGIN in ('RDS_SENDNOTIFICATIONSTATUS','XNA'))      ) trg LEFT JOIN
# MAGIC     (select /*+ full(xc) */      EST_SOURCE_ID, 
# MAGIC        EST_SOURCE_SYSTEM_ID, 
# MAGIC        EST_SOURCE_SYS_ORIGIN, 
# MAGIC        to_date('30000101','yyyyMMdd') as EST_VALID_TO
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_EVENT_STATUS_MAIN xc
# MAGIC     where (EST_SOURCE_SYSTEM_ID in('WBL', 'XNA') and EST_SOURCE_SYS_ORIGIN in ('RDS_SENDNOTIFICATIONSTATUS','XNA'))) src
# MAGIC ON trg.EST_SOURCE_ID = src.EST_SOURCE_ID
# MAGIC  and trg.EST_SOURCE_SYSTEM_ID = src.EST_SOURCE_SYSTEM_ID
# MAGIC  and trg.EST_SOURCE_SYS_ORIGIN = src.EST_SOURCE_SYS_ORIGIN
# MAGIC  and trg.EST_VALID_TO = src.EST_VALID_TO WHERE (src.EST_SOURCE_ID is null);

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
# MAGIC update  gap_catalog.ads_owner.EVENT_STATUS set
# MAGIC        EST_UPDATED_DATETIME = CURRENT_TIMESTAMP(),
# MAGIC        EST_UPDATE_PROCESS_KEY = 13173833,
# MAGIC        EST_CURRENT_FLAG = 'N', 
# MAGIC        EST_VALID_TO = to_date('$p_load_date','yyyy-MM-dd')-1
# MAGIC  where EST_CURRENT_FLAG = 'Y'
# MAGIC    and EST_VALID_TO = to_date('30000101','yyyyMMdd')
# MAGIC    and est_key||'.'||est_valid_from in (select tech_rid from ${var.dif_table_name} where tech_rid is not null);

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.EVENT_STATUS 
# MAGIC  ( EST_KEY, 
# MAGIC    EST_SOURCE_ID, 
# MAGIC    EST_SOURCE_SYSTEM_ID, 
# MAGIC    EST_SOURCE_SYS_ORIGIN, 
# MAGIC    EST_DESC, 
# MAGIC    EST_VALID_FROM, 
# MAGIC    EST_VALID_TO, 
# MAGIC    EST_CURRENT_FLAG, 
# MAGIC    EST_DELETED_FLAG, 
# MAGIC    EST_INSERTED_DATETIME, 
# MAGIC    EST_INSERT_PROCESS_KEY, 
# MAGIC    EST_UPDATED_DATETIME, 
# MAGIC    EST_UPDATE_PROCESS_KEY)
# MAGIC select EST_KEY, 
# MAGIC    EST_SOURCE_ID, 
# MAGIC    EST_SOURCE_SYSTEM_ID, 
# MAGIC    EST_SOURCE_SYS_ORIGIN, 
# MAGIC    EST_DESC, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as EST_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as EST_VALID_TO, 
# MAGIC    'Y' as EST_CURRENT_FLAG, 
# MAGIC    tech_del_flg as EST_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as EST_INSERTED_DATETIME, 
# MAGIC    $p_process_key as EST_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as EST_UPDATED_DATETIME, 
# MAGIC    $p_process_key as EST_UPDATE_PROCESS_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'N';

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.EVENT_STATUS 
# MAGIC  ( EST_KEY, 
# MAGIC    EST_SOURCE_ID, 
# MAGIC    EST_SOURCE_SYSTEM_ID, 
# MAGIC    EST_SOURCE_SYS_ORIGIN, 
# MAGIC    EST_DESC, 
# MAGIC    EST_VALID_FROM, 
# MAGIC    EST_VALID_TO, 
# MAGIC    EST_CURRENT_FLAG, 
# MAGIC    EST_DELETED_FLAG, 
# MAGIC    EST_INSERTED_DATETIME, 
# MAGIC    EST_INSERT_PROCESS_KEY, 
# MAGIC    EST_UPDATED_DATETIME, 
# MAGIC    EST_UPDATE_PROCESS_KEY)
# MAGIC select EST_KEY_NEW as EST_KEY, 
# MAGIC    EST_SOURCE_ID, 
# MAGIC    EST_SOURCE_SYSTEM_ID, 
# MAGIC    EST_SOURCE_SYS_ORIGIN, 
# MAGIC    EST_DESC, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as EST_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as EST_VALID_TO, 
# MAGIC    'Y' as EST_CURRENT_FLAG, 
# MAGIC    tech_del_flg as EST_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as EST_INSERTED_DATETIME, 
# MAGIC    $p_process_key as EST_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as EST_UPDATED_DATETIME, 
# MAGIC    $p_process_key as EST_UPDATE_PROCESS_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'Y';
# MAGIC
