# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE
# MAGIC - Generated from Oracle Import file using IMPD_Convert_Map_Steps_Ora2DBX.py
# MAGIC - Export date: 2025-09-12 19:14:33

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
dbutils.widgets.text("p_process_key", "-999", "Process Key")

map_id = 'ADS_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_owner.DIFF_ADS_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE'

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
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE 
# MAGIC       select /* + PARALLEL(6) */
# MAGIC       				CPPROP_KEY
# MAGIC       			, CPPROP_SOURCE_ID
# MAGIC       			, CPPROP_SOURCE_SYSTEM_ID
# MAGIC       			, CPPROP_SOURCE_SYS_ORIGIN
# MAGIC       			, CPPROP_PARENT_SOURCE_ID
# MAGIC       			, CPPROP_PARENT_KEY
# MAGIC       			, CPPTP_KEY
# MAGIC       			, DUPL_RNK
# MAGIC       	from 
# MAGIC       	( 
# MAGIC with cpp_types as (
# MAGIC select /*+ no_merge */
# MAGIC         t.cpptp_source_id,
# MAGIC         t.cpptp_key,
# MAGIC         substr(t.cpptp_source_id, instr(t.cpptp_source_id, '|') + 1)        as ct,
# MAGIC         length(t.CPPTP_PATH_LIKE)                                           as mask_length,
# MAGIC         substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1)      as cpptp_source_id_99,
# MAGIC         substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1)||'%' as cpptp_source_id_44,
# MAGIC         case -- optimalize pro nahradu liku uvnitr retezce
# MAGIC             when -- pokud je % ve stringu a je na konci retezce tak nic
# MAGIC                     instr(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1),'%') > 0 
# MAGIC             and     instr(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1),'%') = length(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1))
# MAGIC                 then substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1)
# MAGIC       		when -- pokud je % ve stringu a neni na konci retezce tak pridam na konec
# MAGIC                     instr(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1),'%') > 0 
# MAGIC             and     instr(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1),'%') <> length(substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1))
# MAGIC       			then substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1)||'%'
# MAGIC       			else substr(t.cpptp_source_id,1, instr(t.cpptp_source_id, '|') - 1)||'%' -- kdyz nesplnuje ani jednu podminku tak pridej % na konec
# MAGIC       	end                                                                 as cpptp_source_id2
# MAGIC from    gap_catalog.ads_owner.case_phase_property_types t
# MAGIC where   t.cpptp_source_system_id    = 'SMA'
# MAGIC and     t.cpptp_source_sys_origin   = 'SMA_MONITOR_EVENTS'
# MAGIC and     t.cpptp_valid_to            = date '3000-01-01'
# MAGIC ) --select count(*) from CPP_TYPES -- rows 647,906 sec 1
# MAGIC , src as (
# MAGIC select /*+ use_hash(src par) no_merge */
# MAGIC         src.cpprop_key                  as cpprop_key,
# MAGIC         src.cpprop_source_id        ,
# MAGIC         src.cpprop_source_system_id ,
# MAGIC         src.cpprop_source_sys_origin,
# MAGIC         src.cpprop_parent_source_id     as cpprop_parent_source_id,
# MAGIC         nvl(par.cpprop_key,-1)          as cpprop_parent_key,
# MAGIC         src.attr_fp,
# MAGIC         src.casetype,
# MAGIC         src.cpptp_source_id,
# MAGIC         par.cpprop_key                  as par_cpprop_key
# MAGIC             /*
# MAGIC                                 nvl(cpp_types.cpptp_key,-1)     as cpptp_key,
# MAGIC                                 case
# MAGIC                                     when    nvl(par.cpprop_key,-1)      > -1 
# MAGIC                                     or      nvl(cpp_types.cpptp_key,-1) > -1
# MAGIC                                         then 1
# MAGIC                                         else 0
# MAGIC                                 end                             as final_filter,
# MAGIC                                 row_number() over (partition by src.cpprop_key 
# MAGIC                                                     order by cpp_types.mask_length desc) as dupl_rnk
# MAGIC             */
# MAGIC from    gap_catalog.ads_owner.sma_case_phase_properties src
# MAGIC left join gap_catalog.ads_owner.sma_case_phase_properties par 
# MAGIC on      par.cpprop_source_id = src.cpprop_parent_source_id
# MAGIC )  --select count(*) from src -- 8,418,942 rows 5 sec
# MAGIC , gr as ( -- spojeni src_gr a cpp_types pozdei opet spojime se src ...zjednodusujeme casove narocny join src a cpp_types
# MAGIC select /*+ no_merge */
# MAGIC         src_gr.*,
# MAGIC         cpp_types.cpptp_key,
# MAGIC         cpp_types.mask_length
# MAGIC from    (           -- zde je vybrane pouze nutne torzo subquery src pro rychlejsi spojeni s cpp_types
# MAGIC             select
# MAGIC                     substr(src.cpptp_source_id, 1, 1)   as cpptp_source_id_1,
# MAGIC                     upper(src.casetype)                 as casetype,
# MAGIC                     src.attr_fp
# MAGIC             from    src        
# MAGIC             group by
# MAGIC                     substr(src.cpptp_source_id, 1, 1)   ,
# MAGIC                     upper(src.casetype)                 ,
# MAGIC                     src.attr_fp
# MAGIC          ) src_gr      -- 252,877        
# MAGIC left join cpp_types 
# MAGIC on  src_gr.cpptp_source_id_1    = substr(cpp_types.cpptp_source_id, 1, 1)
# MAGIC and src_gr.casetype             =  cpp_types.ct
# MAGIC and src_gr.attr_fp              like cpp_types.cpptp_source_id_99--||'%'
# MAGIC )-- select count(*) from gr -- rows 625,849 15 sec
# MAGIC select 
# MAGIC         x.cpprop_key,
# MAGIC         x.cpprop_source_id,
# MAGIC         x.cpprop_source_system_id,
# MAGIC         x.cpprop_source_sys_origin,
# MAGIC         x.cpprop_parent_source_id,
# MAGIC         x.cpprop_parent_key,
# MAGIC         x.cpptp_key,
# MAGIC         x.dupl_rnk
# MAGIC from (
# MAGIC         select
# MAGIC                 src.cpprop_key                  as cpprop_key,
# MAGIC                 src.cpprop_source_id        ,
# MAGIC                 src.cpprop_source_system_id ,
# MAGIC                 src.cpprop_source_sys_origin,
# MAGIC                 src.cpprop_parent_source_id     as cpprop_parent_source_id,
# MAGIC                 src.cpprop_parent_key,
# MAGIC                 src.attr_fp,
# MAGIC                 src.casetype,
# MAGIC                 src.cpptp_source_id,
# MAGIC                 src.par_cpprop_key,
# MAGIC                 nvl(gr.cpptp_key,-1)     as cpptp_key,
# MAGIC                 case
# MAGIC                     when    nvl(src.par_cpprop_key,-1)      > -1 
# MAGIC                     or      nvl(gr.cpptp_key,-1) > -1
# MAGIC                         then 1
# MAGIC                         else 0
# MAGIC                 end                             as final_filter,
# MAGIC                 row_number() over (partition by src.cpprop_key 
# MAGIC                                     order by gr.mask_length desc) as dupl_rnk
# MAGIC         from    src
# MAGIC         left join gr
# MAGIC         on      substr(src.cpptp_source_id, 1, 1)   = gr.cpptp_source_id_1
# MAGIC         and     upper(src.casetype)                 = gr.casetype
# MAGIC         and     src.attr_fp                         = gr.attr_fp
# MAGIC         ) x
# MAGIC where   x.dupl_rnk      = 1 
# MAGIC and     x.final_filter  = 1
# MAGIC       	)
# MAGIC ;
# MAGIC /*MERGE*/
# MAGIC merge  into gap_catalog.ads_owner.SMA_CASE_PHASE_PROPERTIES trg
# MAGIC using
# MAGIC (select /*+ full(xc) */  CPPROP_KEY, 
# MAGIC    CPPROP_SOURCE_ID, 
# MAGIC    CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    CPPROP_PARENT_SOURCE_ID, 
# MAGIC    CPPROP_PARENT_KEY, 
# MAGIC    CPPTP_KEY, 
# MAGIC    DUPL_RNK
# MAGIC  from gap_catalog.ads_etl_owner.XC_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE xc
# MAGIC ) src
# MAGIC on 
# MAGIC      (trg.CPPROP_KEY = src.CPPROP_KEY
# MAGIC  and trg.CPPROP_SOURCE_ID = src.CPPROP_SOURCE_ID
# MAGIC  and trg.CPPROP_SOURCE_SYSTEM_ID = src.CPPROP_SOURCE_SYSTEM_ID
# MAGIC  and trg.CPPROP_SOURCE_SYS_ORIGIN = src.CPPROP_SOURCE_SYS_ORIGIN)
# MAGIC when matched then update set 
# MAGIC      trg.CPPROP_PARENT_SOURCE_ID = src.CPPROP_PARENT_SOURCE_ID, 
# MAGIC      trg.CPPROP_PARENT_KEY = src.CPPROP_PARENT_KEY, 
# MAGIC      trg.CPPTP_KEY = src.CPPTP_KEY, 
# MAGIC      trg.DUPL_RNK = src.DUPL_RNK, 
# MAGIC      trg.SMCPP_UPDATED_DATETIME = CURRENT_TIMESTAMP(), 
# MAGIC      trg.SMCPP_UPDATE_PROCESS_KEY = 13165092
# MAGIC  where (
# MAGIC      decode( src.CPPROP_PARENT_SOURCE_ID,trg.CPPROP_PARENT_SOURCE_ID,1,0 ) = 0  or
# MAGIC      decode( src.CPPROP_PARENT_KEY,trg.CPPROP_PARENT_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CPPTP_KEY,trg.CPPTP_KEY,1,0 ) = 0  or
# MAGIC      decode( src.DUPL_RNK,trg.DUPL_RNK,1,0 ) = 0) 
# MAGIC when not matched then  insert 
# MAGIC   (trg.CPPROP_KEY, 
# MAGIC    trg.CPPROP_SOURCE_ID, 
# MAGIC    trg.CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    trg.CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    trg.CPPROP_PARENT_SOURCE_ID, 
# MAGIC    trg.CPPROP_PARENT_KEY, 
# MAGIC    trg.CPPTP_KEY, 
# MAGIC    trg.DUPL_RNK, 
# MAGIC    trg.SMCPP_INSERTED_DATETIME, 
# MAGIC    trg.SMCPP_INSERT_PROCESS_KEY, 
# MAGIC    trg.SMCPP_UPDATED_DATETIME, 
# MAGIC    trg.SMCPP_UPDATE_PROCESS_KEY
# MAGIC  ) 
# MAGIC       values 
# MAGIC   (src.CPPROP_KEY, 
# MAGIC    src.CPPROP_SOURCE_ID, 
# MAGIC    src.CPPROP_SOURCE_SYSTEM_ID, 
# MAGIC    src.CPPROP_SOURCE_SYS_ORIGIN, 
# MAGIC    src.CPPROP_PARENT_SOURCE_ID, 
# MAGIC    src.CPPROP_PARENT_KEY, 
# MAGIC    src.CPPTP_KEY, 
# MAGIC    src.DUPL_RNK, 
# MAGIC    CURRENT_TIMESTAMP(), 
# MAGIC    13165092, 
# MAGIC    CURRENT_TIMESTAMP(), 
# MAGIC    13165092)
