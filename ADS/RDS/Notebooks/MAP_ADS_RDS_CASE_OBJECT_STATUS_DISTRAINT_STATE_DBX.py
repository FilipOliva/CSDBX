# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE
# MAGIC - Generated from Oracle Import file using IMPD_Convert_Map_Steps_Ora2DBX.py
# MAGIC - Export date: 2025-09-16 12:41:49

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

map_id = 'ADS_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_etl_owner.DIFF_ADS_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE_ADS_MAP_SCD_DIFF'

# Get the maximum COS_KEY from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX(cos_key), 0) as max_key 
    FROM gap_catalog.ads_owner.cos
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum COS_KEY: {max_key}")

spark.sql(f"SET var.dif_table_name = {dif_table}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
p_process_key = dbutils.widgets.get("p_process_key")
print("p_load_date: "+p_load_date)
print("p_process_key: "+p_process_key)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE 
# MAGIC       with C_0FILTERADCSLAAK138GRG1J678E1NG6FA as (
# MAGIC       select
# MAGIC           RDS_DISTRAINTSTATE.id  ID,
# MAGIC           RDS_DISTRAINTSTATE.value  VALUE
# MAGIC         from
# MAGIC           gap_catalog.ads_etl_owner.DLK_ADS_LOV_RDS_DISTRAINTSTATE RDS_DISTRAINTSTATE
# MAGIC         where ( CAST(from_utc_timestamp(SYS_EFFECTIVE_DATE, 'Europe/Prague') AS DATE) = '$p_load_date'
# MAGIC             and RDS_DISTRAINTSTATE.std_deleted_flag = 'N'
# MAGIC             and RDS_DISTRAINTSTATE.sys = 'Brasil'
# MAGIC             and RDS_DISTRAINTSTATE.lang = 'CZ'
# MAGIC               )
# MAGIC       )
# MAGIC       select  /*+no hint*/
# MAGIC               /*seznam vkladanych nebo updatovanych sloupcu bez SK, auditnich atribudu a deleted flagu*/
# MAGIC           FILTER_A.ID as COS_SOURCE_ID,
# MAGIC           'RDS' as COS_SOURCE_SYSTEM_ID,
# MAGIC           'rds_distraintstate' as COS_SOURCE_SYS_ORIGIN,
# MAGIC           FILTER_A.VALUE as COS_DESC,
# MAGIC           'N' as COS_DELETED_FLAG
# MAGIC         from (C_0FILTERADCSLAAK138GRG1J678E1NG6FA FILTER_A)
# MAGIC         where (1=1)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gap_catalog.ads_etl_owner.DIFF_ADS_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE_ADS_MAP_SCD_DIFF;

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
# MAGIC create  table gap_catalog.ads_etl_owner.DIFF_ADS_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE_ADS_MAP_SCD_DIFF
# MAGIC ( tech_del_flg  char(1),
# MAGIC   tech_new_rec  char(1),
# MAGIC   tech_rid      varchar(255),
# MAGIC   COS_KEY  INTEGER,
# MAGIC   COS_KEY_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${var.max_key} INCREMENT BY 1),
# MAGIC   COS_SOURCE_ID  varchar(120),
# MAGIC   COS_SOURCE_SYSTEM_ID  varchar(120),
# MAGIC   COS_SOURCE_SYS_ORIGIN  varchar(120),
# MAGIC   COS_DESC  varchar(400))
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   COS_KEY,
# MAGIC   COS_SOURCE_ID,
# MAGIC   COS_SOURCE_SYSTEM_ID,
# MAGIC   COS_SOURCE_SYS_ORIGIN,
# MAGIC   COS_DESC
# MAGIC )
# MAGIC select   'N' as tech_del_flg, 
# MAGIC     case when trg.COS_SOURCE_ID is null then 'Y' else 'N' end as tech_new_rec,
# MAGIC     trg.rid as tech_rid,
# MAGIC    trg.COS_KEY, 
# MAGIC    src.COS_SOURCE_ID, 
# MAGIC    src.COS_SOURCE_SYSTEM_ID, 
# MAGIC    src.COS_SOURCE_SYS_ORIGIN, 
# MAGIC    src.COS_DESC
# MAGIC  from 
# MAGIC     (select /*+ full(xc) */      COS_SOURCE_ID, 
# MAGIC        COS_SOURCE_SYSTEM_ID, 
# MAGIC        COS_SOURCE_SYS_ORIGIN, 
# MAGIC        COS_DESC, 
# MAGIC        COS_DELETED_FLAG
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE xc
# MAGIC     where (COS_SOURCE_SYS_ORIGIN = 'rds_distraintstate'
# MAGIC           and COS_SOURCE_SYSTEM_ID = 'RDS')) src LEFT JOIN
# MAGIC     (select  cos_key||'.'||cos_valid_from as rid, t.* from gap_catalog.ads_owner.CASE_OBJECT_STATUS t
# MAGIC       where COS_CURRENT_FLAG  = 'Y'
# MAGIC         and COS_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (COS_SOURCE_SYS_ORIGIN = 'rds_distraintstate'
# MAGIC           and COS_SOURCE_SYSTEM_ID = 'RDS')      ) trg
# MAGIC ON trg.COS_SOURCE_ID = src.COS_SOURCE_ID
# MAGIC  and trg.COS_SOURCE_SYSTEM_ID = src.COS_SOURCE_SYSTEM_ID
# MAGIC  and trg.COS_SOURCE_SYS_ORIGIN = src.COS_SOURCE_SYS_ORIGIN
# MAGIC  and trg.COS_VALID_TO = to_date('30000101','yyyyMMdd') WHERE (
# MAGIC      decode( src.COS_DESC,trg.COS_DESC,1,0 ) = 0  or
# MAGIC      decode( src.COS_DELETED_FLAG,trg.COS_DELETED_FLAG,1,0 ) = 0 or 
# MAGIC      trg.COS_SOURCE_ID is null   );

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
# MAGIC insert into ${var.dif_table_name}
# MAGIC (
# MAGIC   tech_del_flg,
# MAGIC   tech_new_rec,
# MAGIC   tech_rid,
# MAGIC   COS_KEY,
# MAGIC   COS_SOURCE_ID,
# MAGIC   COS_SOURCE_SYSTEM_ID,
# MAGIC   COS_SOURCE_SYS_ORIGIN,
# MAGIC   COS_DESC
# MAGIC )
# MAGIC select   'Y' as tech_del_flg, 
# MAGIC  'N' as tech_new_rec, 
# MAGIC  trg.rid as tech_rid, 
# MAGIC    trg.COS_KEY, 
# MAGIC    trg.COS_SOURCE_ID, 
# MAGIC    trg.COS_SOURCE_SYSTEM_ID, 
# MAGIC    trg.COS_SOURCE_SYS_ORIGIN, 
# MAGIC    trg.COS_DESC
# MAGIC  from 
# MAGIC     (select  cos_key||'.'||cos_valid_from as rid, t.* from gap_catalog.ads_owner.CASE_OBJECT_STATUS t
# MAGIC       where COS_CURRENT_FLAG  = 'Y' and COS_DELETED_FLAG  = 'N'
# MAGIC         and COS_VALID_TO  = to_date('01013000','ddMMyyyy')
# MAGIC        and (COS_SOURCE_SYS_ORIGIN = 'rds_distraintstate'
# MAGIC           and COS_SOURCE_SYSTEM_ID = 'RDS')      ) trg LEFT JOIN
# MAGIC     (select /*+ full(xc) */      COS_SOURCE_ID, 
# MAGIC        COS_SOURCE_SYSTEM_ID, 
# MAGIC        COS_SOURCE_SYS_ORIGIN, 
# MAGIC        to_date('30000101','yyyyMMdd') as COS_VALID_TO
# MAGIC        from gap_catalog.ads_etl_owner.XC_RDS_CASE_OBJECT_STATUS_DISTRAINT_STATE xc
# MAGIC     where (COS_SOURCE_SYS_ORIGIN = 'rds_distraintstate'
# MAGIC           and COS_SOURCE_SYSTEM_ID = 'RDS')) src
# MAGIC ON trg.COS_SOURCE_ID = src.COS_SOURCE_ID
# MAGIC  and trg.COS_SOURCE_SYSTEM_ID = src.COS_SOURCE_SYSTEM_ID
# MAGIC  and trg.COS_SOURCE_SYS_ORIGIN = src.COS_SOURCE_SYS_ORIGIN
# MAGIC  and trg.COS_VALID_TO = src.COS_VALID_TO WHERE (src.COS_SOURCE_ID is null);

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
# MAGIC update  gap_catalog.ads_owner.CASE_OBJECT_STATUS set
# MAGIC        COS_UPDATED_DATETIME = CURRENT_TIMESTAMP(),
# MAGIC        COS_UPDATE_PROCESS_KEY = 13173833,
# MAGIC        COS_CURRENT_FLAG = 'N', 
# MAGIC        COS_VALID_TO = to_date('$p_load_date','yyyy-MM-dd')-1
# MAGIC  where COS_CURRENT_FLAG = 'Y'
# MAGIC    and COS_VALID_TO = to_date('30000101','yyyyMMdd')
# MAGIC    and cos_key||'.'||cos_valid_from in (select tech_rid from ${var.dif_table_name} where tech_rid is not null);

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.CASE_OBJECT_STATUS 
# MAGIC  ( COS_KEY, 
# MAGIC    COS_SOURCE_ID, 
# MAGIC    COS_SOURCE_SYSTEM_ID, 
# MAGIC    COS_SOURCE_SYS_ORIGIN, 
# MAGIC    COS_DESC, 
# MAGIC    COS_VALID_FROM, 
# MAGIC    COS_VALID_TO, 
# MAGIC    COS_CURRENT_FLAG, 
# MAGIC    COS_DELETED_FLAG, 
# MAGIC    COS_INSERTED_DATETIME, 
# MAGIC    COS_INSERT_PROCESS_KEY, 
# MAGIC    COS_UPDATED_DATETIME, 
# MAGIC    COS_UPDATE_PROCESS_KEY)
# MAGIC select COS_KEY, 
# MAGIC    COS_SOURCE_ID, 
# MAGIC    COS_SOURCE_SYSTEM_ID, 
# MAGIC    COS_SOURCE_SYS_ORIGIN, 
# MAGIC    COS_DESC, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as COS_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as COS_VALID_TO, 
# MAGIC    'Y' as COS_CURRENT_FLAG, 
# MAGIC    tech_del_flg as COS_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as COS_INSERTED_DATETIME, 
# MAGIC    $p_process_key as COS_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as COS_UPDATED_DATETIME, 
# MAGIC    $p_process_key as COS_UPDATE_PROCESS_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'N';

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
# MAGIC insert  into gap_catalog.ads_owner.CASE_OBJECT_STATUS 
# MAGIC  ( COS_KEY, 
# MAGIC    COS_SOURCE_ID, 
# MAGIC    COS_SOURCE_SYSTEM_ID, 
# MAGIC    COS_SOURCE_SYS_ORIGIN, 
# MAGIC    COS_DESC, 
# MAGIC    COS_VALID_FROM, 
# MAGIC    COS_VALID_TO, 
# MAGIC    COS_CURRENT_FLAG, 
# MAGIC    COS_DELETED_FLAG, 
# MAGIC    COS_INSERTED_DATETIME, 
# MAGIC    COS_INSERT_PROCESS_KEY, 
# MAGIC    COS_UPDATED_DATETIME, 
# MAGIC    COS_UPDATE_PROCESS_KEY)
# MAGIC select gap_catalog.ads_owner.CASE_OBJECT_STATUS_S.nextval as COS_KEY, 
# MAGIC    COS_SOURCE_ID, 
# MAGIC    COS_SOURCE_SYSTEM_ID, 
# MAGIC    COS_SOURCE_SYS_ORIGIN, 
# MAGIC    COS_DESC, 
# MAGIC    to_date('$p_load_date','yyyy-MM-dd') as COS_VALID_FROM, 
# MAGIC    to_date('3000-01-01','yyyy-MM-dd') as COS_VALID_TO, 
# MAGIC    'Y' as COS_CURRENT_FLAG, 
# MAGIC    tech_del_flg as COS_DELETED_FLAG, 
# MAGIC    CURRENT_TIMESTAMP() as COS_INSERTED_DATETIME, 
# MAGIC    $p_process_key as COS_INSERT_PROCESS_KEY, 
# MAGIC    CURRENT_TIMESTAMP() as COS_UPDATED_DATETIME, 
# MAGIC    $p_process_key as COS_UPDATE_PROCESS_KEY
# MAGIC   from ${var.dif_table_name}
# MAGIC  where tech_new_rec = 'Y';
# MAGIC
