# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps ADS_SMA_PROCESS_EVENTS_MAIN
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
dbutils.widgets.text("p_process_key", "-999", "Process Key")

map_id = 'ADS_SMA_PROCESS_EVENTS_MAIN'
schema_name = 'gap_catalog.ads_owner'
dif_table = 'gap_catalog.ads_owner.DIFF_ADS_SMA_PROCESS_EVENTS_MAIN'

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
# MAGIC %sql truncate table gap_catalog.ads_etl_owner.XC_SMA_PROCESS_EVENTS_MAIN

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
# MAGIC INSERT INTO gap_catalog.ads_etl_owner.XC_SMA_PROCESS_EVENTS_MAIN 
# MAGIC             with ca0 as (
# MAGIC             select 
# MAGIC               s.eventid,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) dt_timestamp,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) t_timestamp,
# MAGIC               s.type type,
# MAGIC               s.cidla,
# MAGIC               cp.caseph_key,
# MAGIC               CP.case_key,
# MAGIC               s."USER" as "USER",
# MAGIC               HUMANTASKCLUID AS CLUID,
# MAGIC               TAKETYPE as taketype,
# MAGIC               null as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             left join gap_catalog.ads_owner.case_phases cp on s.HUMANTASKID = cp.caseph_source_id
# MAGIC             and  cp.caseph_source_system_id = 'SMA'
# MAGIC             and  cp.caseph_source_sys_origin = 'SMA_MONITOR_EVENTS'
# MAGIC             and cp.cpcat_key=2 and cp.CASEPH_VALID_TO=date'3000-01-01'
# MAGIC             where
# MAGIC             s.type like ('HT_%') or
# MAGIC             s.type in ('REQUISITION_CREATED','REQUISITION_COMPLETED')
# MAGIC             union  all
# MAGIC             select 
# MAGIC               s.eventid,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) dt_timestamp,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) t_timestamp,
# MAGIC               s.type type,
# MAGIC               s.cidla,
# MAGIC               a.caseph_key as caseph_key,
# MAGIC               nvl(a.case_key, -1) as case_key,
# MAGIC               s."USER" as "USER",
# MAGIC               coalesce(THECASECLUID, HUMANTASKCLUID, GEORGETASKCLUID) as cluid,
# MAGIC               TAKETYPE as taketype,
# MAGIC               null as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             left join gap_catalog.ads_owner.case_phases a 
# MAGIC             on a.caseph_source_id= s.SERVICEID
# MAGIC             and  a.caseph_source_system_id = 'SMA'
# MAGIC             and  a.caseph_source_sys_origin = 'SMA_MONITOR_EVENTS'
# MAGIC             and a.cpcat_key=6 and a.CASEPH_VALID_TO=date'3000-01-01'
# MAGIC             where s.type like ('ACTIVITY_%')
# MAGIC             union  all
# MAGIC             select
# MAGIC               s.eventid,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) dt_timestamp,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) t_timestamp,
# MAGIC               s.type type,
# MAGIC               s.cidla,
# MAGIC               CT.caseph_key as caseph_key,
# MAGIC               nvl(CT.case_key, -1) as case_key,
# MAGIC               s."USER" as "USER",
# MAGIC               coalesce(THECASECLUID, HUMANTASKCLUID, GEORGETASKCLUID) as cluid,
# MAGIC               TAKETYPE as taketype,
# MAGIC               null as eve_status_desc              
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             left join gap_catalog.ads_owner.case_phases ct on CLIENTTASKID = ct.caseph_source_id
# MAGIC             and  ct.caseph_source_system_id = 'SMA'
# MAGIC             and  ct.caseph_source_sys_origin = 'SMA_MONITOR_EVENTS'
# MAGIC             and ct.cpcat_key=7 and ct.CASEPH_VALID_TO=date'3000-01-01'
# MAGIC             where s.type like ('CLIENT_TASK%')
# MAGIC             union  all
# MAGIC             select 
# MAGIC               s.eventid,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) dt_timestamp,
# MAGIC               unix_ts_to_date(TIMESTAMP/1000) t_timestamp,
# MAGIC               s.type type,
# MAGIC               s.cidla,
# MAGIC               Ca.caseph_key as caseph_key,
# MAGIC               nvl(CA.case_key, -1) as case_key,
# MAGIC               s."USER" as "USER",
# MAGIC              coalesce(THECASECLUID, HUMANTASKCLUID, GEORGETASKCLUID) as cluid,
# MAGIC               TAKETYPE as taketype,
# MAGIC               null as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             left join gap_catalog.ads_owner.case_phases ca on s.CLIENTACTIVITYID = ca.caseph_source_id
# MAGIC             and  ca.caseph_source_system_id = 'SMA'
# MAGIC             and  ca.caseph_source_sys_origin = 'SMA_MONITOR_EVENTS'
# MAGIC             and ca.cpcat_key=8 and ca.CASEPH_VALID_TO=date'3000-01-01'
# MAGIC             where s.type like ('CLIENT_ACTIVITY%')
# MAGIC             union  all
# MAGIC             select
# MAGIC               s.eventid,
# MAGIC               unix_ts_to_date(s.TIMESTAMP/1000) dt_timestamp,
# MAGIC               unix_ts_to_date(s.TIMESTAMP/1000) t_timestamp,
# MAGIC               s.type type,
# MAGIC               s.cidla,
# MAGIC               GT.caseph_key as caseph_key,
# MAGIC               nvl(GT.case_key, -1) as case_key,
# MAGIC               s."USER" as "USER",
# MAGIC              coalesce(s.THECASECLUID, s.HUMANTASKCLUID, s.GEORGETASKCLUID) as cluid,
# MAGIC               s.TAKETYPE as taketype,
# MAGIC               null as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             left join gap_catalog.ads_owner.case_phases gt on s.GEORGETASKID = gt.caseph_source_id
# MAGIC             and  gt.caseph_source_system_id = 'SMA'
# MAGIC             and  gt.caseph_source_sys_origin = 'SMA_MONITOR_EVENTS'
# MAGIC             and gt.cpcat_key=314 and gt.CASEPH_VALID_TO=date'3000-01-01' 
# MAGIC             where s.type like ('GEORGE%')
# MAGIC             union all
# MAGIC             select 
# MAGIC                 s.eventid,
# MAGIC                 unix_ts_to_date(s.TIMESTAMP/1000) dt_timestamp,
# MAGIC                 unix_ts_to_date(s.TIMESTAMP/1000) t_timestamp,
# MAGIC                 s.type type,
# MAGIC                 s.cidla,
# MAGIC                 -1 as caseph_key,
# MAGIC                 NULL case_key,
# MAGIC                 s."USER" as "USER",
# MAGIC                 coalesce(s.THECASECLUID, s.HUMANTASKCLUID, s.GEORGETASKCLUID) as cluid,
# MAGIC                 s.TAKETYPE as taketype,
# MAGIC               s.label as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             where  
# MAGIC              S.TYPE LIKE ('RUNNER%')
# MAGIC            union all
# MAGIC             select 
# MAGIC                 s.eventid,
# MAGIC                 unix_ts_to_date(s.TIMESTAMP/1000) dt_timestamp,
# MAGIC                 unix_ts_to_date(s.TIMESTAMP/1000) t_timestamp,
# MAGIC                 s.type type,
# MAGIC                 s.cidla,
# MAGIC                 -1 as caseph_key,
# MAGIC                 NULL case_key,
# MAGIC                 s."USER" as "USER",
# MAGIC                 coalesce(s.THECASECLUID, s.HUMANTASKCLUID, s.GEORGETASKCLUID) as cluid,
# MAGIC                 s.TAKETYPE as taketype,
# MAGIC               null as eve_status_desc
# MAGIC             from gap_catalog.ads_etl_owner.STG_SMA_PROCESS_EVENTS_PARSE s
# MAGIC             where s.type like ('CASE%') 
# MAGIC             ) 
# MAGIC             select
# MAGIC               ca0.eventid as eve_source_id,
# MAGIC               'SMA' as eve_source_system_id,
# MAGIC               'SMA_MONITOR_EVENTS' as eve_source_sys_origin,
# MAGIC               nvl(P1.PT_KEY, -1) as pt_key,
# MAGIC               nvl(P1.PTORI_KEY, -1) as ptori_key,
# MAGIC               nvl(p1.PT_UNIFIED_KEY, -1) as pt_unified_key,
# MAGIC               nvl(p.PT_KEY,-1) as pt_employee_key,
# MAGIC               nvl(p.PTORI_KEY,-1) as ptori_employee_key,
# MAGIC               'XNA' as id_financial_product,
# MAGIC               ca0.caseph_key,
# MAGIC               -1 as ntp_key,
# MAGIC               -1 as chan_key,
# MAGIC               nvl(et.evetp_key, -1) as evetp_key,
# MAGIC               -1 as est_key,
# MAGIC               coalesce(c.case_key, ca0.case_key, -1) as case_key,
# MAGIC               trunc(ca0.dt_timestamp) as eve_date,
# MAGIC               ca0.t_timestamp as eve_datetime,
# MAGIC               ca0.t_timestamp as eve_recorded_date,
# MAGIC               'XNA' as proce_target,
# MAGIC               nvl(sr.str_key, -1) as str_key,
# MAGIC               nvl(a.CGP_ID, 'XNA') as cgp_id, nvl(a.ACC_KEY, -1) as acc_key, nvl(a.ACCTP_KEY, -1) as acctp_key,
# MAGIC               nvl(a.CARD_KEY, -1) as card_key, nvl(a.SRV_KEY, -1) as srv_key, nvl(c.CASE_MASTER_KEY, -1) as case_master_key, -1 as doc_key,
# MAGIC               -1 as docl_key ,
# MAGIC               eve_status_desc
# MAGIC             from ca0
# MAGIC             left join gap_catalog.ads_owner.cases c on ca0.cidla = c.case_source_id  and c.case_source_system_id = 'SMA' and c.case_source_sys_origin = 'SMA_MONITOR_EVENTS' and c.case_valid_to=date'3000-01-01'
# MAGIC             left join dwh_owner.parties p on upper(ca0."USER")=p.pt_source_id and p.ptori_key = 32 and p.pt_source_system_id='LD0'
# MAGIC             left join dwh_owner.parties p1 on p1.PT_UNIQUE_PARTY_ID = ca0.cluid and p1.ptori_key = 4 and p1.pt_source_system_id='CR0'
# MAGIC             left join gap_catalog.ads_owner.event_types et on et.evetp_source_id = ca0.type and et.evetp_source_system_id = 'SMA' and  et.evetp_source_sys_origin = 'SMA_SYSTEM_EVENTS' and et.evetp_valid_to=date'3000-01-01' 
# MAGIC             left join gap_catalog.ads_owner.status_reason sr on ca0.taketype= sr.STR_SOURCE_ID and  sr.STR_SOURCE_SYSTEM_ID='SMA' and  sr.STR_SOURCE_SYS_ORIGIN='SMA_SYSTEM_EVENTS.TAKE_TYPE' and  sr.STR_VALID_TO =date'3000-01-01'
# MAGIC             left join (SELECT 
# MAGIC                 PROD_INST_CASES.CASE_KEY AS CASE_KEY ,
# MAGIC                 PROD_INST_CASES.ACC_KEY AS ACC_KEY ,
# MAGIC                 PROD_INST_CASES.ACCTP_KEY AS ACCTP_KEY ,
# MAGIC                 PROD_INST_CASES.CARD_KEY AS CARD_KEY ,
# MAGIC                 PROD_INST_CASES.SRV_KEY AS SRV_KEY ,
# MAGIC                 PROD_INST_CASES.CGP_ID AS CGP_ID ,
# MAGIC                 rank() over (partition by PROD_INST_CASES.CASE_KEY order by PROD_INST_CASES.ACC_KEY desc, PROD_INST_CASES.CARD_KEY desc, case when PROD_INST_CASES.CGP_ID <> 'XNA' then 0 else 10 end, PROD_INST_CASES.PRICASE_VALID_FROM) AS rn   
# MAGIC               FROM 
# MAGIC                 gap_catalog.ads_owner.PROD_INST_CASES PROD_INST_CASES  INNER JOIN  gap_catalog.ads_owner.PROD_INST_CASE_TYPES PROD_INST_CASE_TYPES  
# MAGIC                   ON  PROD_INST_CASES.PRICASETP_KEY = PROD_INST_CASE_TYPES.PRICASETP_KEY
# MAGIC               WHERE
# MAGIC                 (to_date('20250911','yyyymmdd') between PROD_INST_CASES.PRICASE_VALID_from and PROD_INST_CASES.PRICASE_VALID_TO
# MAGIC               ) AND   (PROD_INST_CASE_TYPES.PRICASETP_VALID_TO = date '3000-01-01'
# MAGIC               and PROD_INST_CASE_TYPES.PRICASETP_SOURCE_SYSTEM_ID = 'ADS'
# MAGIC               and PROD_INST_CASE_TYPES.PRICASETP_SOURCE_ID = 'SMA_CASE_CGP')) a on c.case_key=a.case_key and a.rn=1            
# MAGIC ;
# MAGIC /*MERGE*/
# MAGIC merge  into gap_catalog.ads_owner.PROCESS_EVENTS trg
# MAGIC using
# MAGIC (select /*+ full(xc) */  EVE_SOURCE_ID, 
# MAGIC    EVE_SOURCE_SYSTEM_ID, 
# MAGIC    EVE_SOURCE_SYS_ORIGIN, 
# MAGIC    PT_KEY, 
# MAGIC    PTORI_KEY, 
# MAGIC    PT_UNIFIED_KEY, 
# MAGIC    PT_EMPLOYEE_KEY, 
# MAGIC    PTORI_EMPLOYEE_KEY, 
# MAGIC    ID_FINANCIAL_PRODUCT, 
# MAGIC    CASEPH_KEY, 
# MAGIC    NTP_KEY, 
# MAGIC    CHAN_KEY, 
# MAGIC    EVETP_KEY, 
# MAGIC    EST_KEY, 
# MAGIC    CASE_KEY, 
# MAGIC    CGP_ID, 
# MAGIC    EVE_DATE, 
# MAGIC    EVE_DATETIME, 
# MAGIC    EVE_RECORDED_DATE, 
# MAGIC    PROCE_TARGET, 
# MAGIC    ACC_KEY, 
# MAGIC    ACCTP_KEY, 
# MAGIC    CARD_KEY, 
# MAGIC    SRV_KEY, 
# MAGIC    CASE_MASTER_KEY, 
# MAGIC    DOC_KEY, 
# MAGIC    DOCL_KEY, 
# MAGIC    STR_KEY, 
# MAGIC    EVE_STATUS_DESC
# MAGIC  from gap_catalog.ads_etl_owner.XC_SMA_PROCESS_EVENTS_MAIN xc
# MAGIC  where (EVE_SOURCE_SYS_ORIGIN = 'SMA_MONITOR_EVENTS'
# MAGIC          and EVE_DATE in (to_date('20250911','YYYYMMDD'), to_date('20250911','YYYYMMDD') -1, to_date('20250911','YYYYMMDD')+1)
# MAGIC          and EVE_SOURCE_SYSTEM_ID = 'SMA'
# MAGIC          and EVETP_KEY in (
# MAGIC           /*+ no_unnest push_subq */  Select EVETP_KEY from gap_catalog.ads_owner.EVENT_TYPES
# MAGIC          where
# MAGIC             EVENT_TYPES.EVETP_VALID_TO = date '3000-01-01' and
# MAGIC             EVENT_TYPES.EVETP_SOURCE_SYSTEM_ID = 'SMA' and
# MAGIC             EVENT_TYPES.EVETP_SOURCE_SYS_ORIGIN = 'SMA_SYSTEM_EVENTS'
# MAGIC          ))) src
# MAGIC on 
# MAGIC      (trg.EVE_SOURCE_ID = src.EVE_SOURCE_ID
# MAGIC  and trg.EVE_SOURCE_SYSTEM_ID = src.EVE_SOURCE_SYSTEM_ID
# MAGIC  and trg.EVE_SOURCE_SYS_ORIGIN = src.EVE_SOURCE_SYS_ORIGIN
# MAGIC  and trg.EVETP_KEY = src.EVETP_KEY
# MAGIC  and trg.EVE_DATE = src.EVE_DATE)
# MAGIC when matched then update set 
# MAGIC      trg.PT_KEY = src.PT_KEY, 
# MAGIC      trg.PTORI_KEY = src.PTORI_KEY, 
# MAGIC      trg.PT_UNIFIED_KEY = src.PT_UNIFIED_KEY, 
# MAGIC      trg.PT_EMPLOYEE_KEY = src.PT_EMPLOYEE_KEY, 
# MAGIC      trg.PTORI_EMPLOYEE_KEY = src.PTORI_EMPLOYEE_KEY, 
# MAGIC      trg.ID_FINANCIAL_PRODUCT = src.ID_FINANCIAL_PRODUCT, 
# MAGIC      trg.CASEPH_KEY = src.CASEPH_KEY, 
# MAGIC      trg.NTP_KEY = src.NTP_KEY, 
# MAGIC      trg.CHAN_KEY = src.CHAN_KEY, 
# MAGIC      trg.EST_KEY = src.EST_KEY, 
# MAGIC      trg.CASE_KEY = src.CASE_KEY, 
# MAGIC      trg.CGP_ID = src.CGP_ID, 
# MAGIC      trg.EVE_DATETIME = src.EVE_DATETIME, 
# MAGIC      trg.EVE_RECORDED_DATE = src.EVE_RECORDED_DATE, 
# MAGIC      trg.PROCE_TARGET = src.PROCE_TARGET, 
# MAGIC      trg.PROCE_UPDATED_DATETIME = CURRENT_TIMESTAMP(), 
# MAGIC      trg.PROCE_UPDATE_PROCESS_KEY = 13165092, 
# MAGIC      trg.ACC_KEY = src.ACC_KEY, 
# MAGIC      trg.ACCTP_KEY = src.ACCTP_KEY, 
# MAGIC      trg.CARD_KEY = src.CARD_KEY, 
# MAGIC      trg.SRV_KEY = src.SRV_KEY, 
# MAGIC      trg.CASE_MASTER_KEY = src.CASE_MASTER_KEY, 
# MAGIC      trg.DOC_KEY = src.DOC_KEY, 
# MAGIC      trg.DOCL_KEY = src.DOCL_KEY, 
# MAGIC      trg.STR_KEY = src.STR_KEY, 
# MAGIC      trg.EVE_STATUS_DESC = src.EVE_STATUS_DESC
# MAGIC  where (
# MAGIC      decode( src.PT_KEY,trg.PT_KEY,1,0 ) = 0  or
# MAGIC      decode( src.PTORI_KEY,trg.PTORI_KEY,1,0 ) = 0  or
# MAGIC      decode( src.PT_UNIFIED_KEY,trg.PT_UNIFIED_KEY,1,0 ) = 0  or
# MAGIC      decode( src.PT_EMPLOYEE_KEY,trg.PT_EMPLOYEE_KEY,1,0 ) = 0  or
# MAGIC      decode( src.PTORI_EMPLOYEE_KEY,trg.PTORI_EMPLOYEE_KEY,1,0 ) = 0  or
# MAGIC      decode( src.ID_FINANCIAL_PRODUCT,trg.ID_FINANCIAL_PRODUCT,1,0 ) = 0  or
# MAGIC      decode( src.CASEPH_KEY,trg.CASEPH_KEY,1,0 ) = 0  or
# MAGIC      decode( src.NTP_KEY,trg.NTP_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CHAN_KEY,trg.CHAN_KEY,1,0 ) = 0  or
# MAGIC      decode( src.EST_KEY,trg.EST_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CASE_KEY,trg.CASE_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CGP_ID,trg.CGP_ID,1,0 ) = 0  or
# MAGIC      decode( src.EVE_DATETIME,trg.EVE_DATETIME,1,0 ) = 0  or
# MAGIC      decode( src.EVE_RECORDED_DATE,trg.EVE_RECORDED_DATE,1,0 ) = 0  or
# MAGIC      decode( src.PROCE_TARGET,trg.PROCE_TARGET,1,0 ) = 0  or
# MAGIC      decode( src.ACC_KEY,trg.ACC_KEY,1,0 ) = 0  or
# MAGIC      decode( src.ACCTP_KEY,trg.ACCTP_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CARD_KEY,trg.CARD_KEY,1,0 ) = 0  or
# MAGIC      decode( src.SRV_KEY,trg.SRV_KEY,1,0 ) = 0  or
# MAGIC      decode( src.CASE_MASTER_KEY,trg.CASE_MASTER_KEY,1,0 ) = 0  or
# MAGIC      decode( src.DOC_KEY,trg.DOC_KEY,1,0 ) = 0  or
# MAGIC      decode( src.DOCL_KEY,trg.DOCL_KEY,1,0 ) = 0  or
# MAGIC      decode( src.STR_KEY,trg.STR_KEY,1,0 ) = 0  or
# MAGIC      decode( src.EVE_STATUS_DESC,trg.EVE_STATUS_DESC,1,0 ) = 0) 
# MAGIC when not matched then  insert 
# MAGIC   (trg.EVE_KEY, 
# MAGIC    trg.EVE_SOURCE_ID, 
# MAGIC    trg.EVE_SOURCE_SYSTEM_ID, 
# MAGIC    trg.EVE_SOURCE_SYS_ORIGIN, 
# MAGIC    trg.PT_KEY, 
# MAGIC    trg.PTORI_KEY, 
# MAGIC    trg.PT_UNIFIED_KEY, 
# MAGIC    trg.PT_EMPLOYEE_KEY, 
# MAGIC    trg.PTORI_EMPLOYEE_KEY, 
# MAGIC    trg.ID_FINANCIAL_PRODUCT, 
# MAGIC    trg.CASEPH_KEY, 
# MAGIC    trg.NTP_KEY, 
# MAGIC    trg.CHAN_KEY, 
# MAGIC    trg.EVETP_KEY, 
# MAGIC    trg.EST_KEY, 
# MAGIC    trg.CASE_KEY, 
# MAGIC    trg.CGP_ID, 
# MAGIC    trg.EVE_DATE, 
# MAGIC    trg.EVE_DATETIME, 
# MAGIC    trg.EVE_RECORDED_DATE, 
# MAGIC    trg.PROCE_TARGET, 
# MAGIC    trg.PROCE_INSERTED_DATETIME, 
# MAGIC    trg.PROCE_INSERT_PROCESS_KEY, 
# MAGIC    trg.PROCE_UPDATED_DATETIME, 
# MAGIC    trg.PROCE_UPDATE_PROCESS_KEY, 
# MAGIC    trg.ACC_KEY, 
# MAGIC    trg.ACCTP_KEY, 
# MAGIC    trg.CARD_KEY, 
# MAGIC    trg.SRV_KEY, 
# MAGIC    trg.CASE_MASTER_KEY, 
# MAGIC    trg.DOC_KEY, 
# MAGIC    trg.DOCL_KEY, 
# MAGIC    trg.STR_KEY, 
# MAGIC    trg.EVE_STATUS_DESC
# MAGIC  ) 
# MAGIC       values 
# MAGIC   (gap_catalog.ads_owner.PROCESS_EVENTS_S.nextval, 
# MAGIC    src.EVE_SOURCE_ID, 
# MAGIC    src.EVE_SOURCE_SYSTEM_ID, 
# MAGIC    src.EVE_SOURCE_SYS_ORIGIN, 
# MAGIC    src.PT_KEY, 
# MAGIC    src.PTORI_KEY, 
# MAGIC    src.PT_UNIFIED_KEY, 
# MAGIC    src.PT_EMPLOYEE_KEY, 
# MAGIC    src.PTORI_EMPLOYEE_KEY, 
# MAGIC    src.ID_FINANCIAL_PRODUCT, 
# MAGIC    src.CASEPH_KEY, 
# MAGIC    src.NTP_KEY, 
# MAGIC    src.CHAN_KEY, 
# MAGIC    src.EVETP_KEY, 
# MAGIC    src.EST_KEY, 
# MAGIC    src.CASE_KEY, 
# MAGIC    src.CGP_ID, 
# MAGIC    src.EVE_DATE, 
# MAGIC    src.EVE_DATETIME, 
# MAGIC    src.EVE_RECORDED_DATE, 
# MAGIC    src.PROCE_TARGET, 
# MAGIC    CURRENT_TIMESTAMP(), 
# MAGIC    13165092, 
# MAGIC    CURRENT_TIMESTAMP(), 
# MAGIC    13165092, 
# MAGIC    src.ACC_KEY, 
# MAGIC    src.ACCTP_KEY, 
# MAGIC    src.CARD_KEY, 
# MAGIC    src.SRV_KEY, 
# MAGIC    src.CASE_MASTER_KEY, 
# MAGIC    src.DOC_KEY, 
# MAGIC    src.DOCL_KEY, 
# MAGIC    src.STR_KEY, 
# MAGIC    src.EVE_STATUS_DESC)
