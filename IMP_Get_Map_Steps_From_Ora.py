#!/usr/bin/env python3
"""
Oracle ETL Mappings Export Script
Exports ETL mapping SQL commands to individual files
Successfully exports from Oracle DWHP database
"""

import os
import oracledb
import re
from pathlib import Path
import pandas as pd

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# Output directory for generated SQL files
output_dir = "IMP_Map_Ora"

# ETL Mappings to process - easy to maintain and modify
MAPPING_LIST = [
    'ADS_STG_SMA_CASE_PHASE_PROPERTIES_MAIN',
    'ADS_STG_SMA_CASE_PHASE_PROPERTIES_UPDATE',
    'ADS_SMA_PROCESS_EVENTS_MAIN',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_0',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_1',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_2',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_3',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_4',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_5',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_6',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_7',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_8',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_9',
    'ADS_SMA_CASE_PHASE_PROPERTIES_MAIN_BUSY_BANKING',
    'ADS_SMA_CASE_PHASE_PROPERTIES_HASHID',
    'ADS_SMA_CASE_PHASE_PROPERTIES_POT_OWN',
    'ADS_SMA_CASE_PHASE_PROPERTIES_HUMANTASK',
    'ADS_RDS_EVENT_STATUS_DISTRAINT_ACT_STATE',
    'ADS_RDS_EVENT_STATUS_MAIN',
    'ADS_RDS_EVENT_STATUS_ONB_PROCESSRESULT',
    'ADS_RDS_EVENT_STATUS_ONB_ELIGIBILITYCHECKRESULT',
    'ADS_RDS_EVENT_STATUS_REG_STATE',
    'ADS_RDS_EVENT_STATUS_MEP',
    'ADS_RDS_EVENT_STATUS_IDENTIF_STATE',
    'ADS_RDS_EVENT_STATUS_ANALYTICAL',
    'ADS_RDS_EVENT_STATUS_ERRORMESSAGEBS',
    'ADS_RDS_EVENT_STATUS_ONB_IDENTIFICATIONRESULT',
    'ADS_RDS_EVENT_TYPES_ERRORMESSAGEBS',
    'ADS_RDS_EVENT_TYPES_DISTRAINT_EVENT_TYPE',
    'ADS_RDS_EVENT_TYPES_REGISTRATIONACTION',
    'ADS_RDS_EVENT_TYPES_MAIN',
    'ADS_RDS_EVENT_TYPES_DISTRAINT_ACT_TYPE',
    'ADS_RDS_EVENT_TYPES_HYPO',
    'ADS_RDS_EVENT_TYPES_DISPOTRANSACTIONTYPE',
    'ADS_RDS_EVENT_TYPES_IDENTIFICATIONACTION',
    'ADS_RDS_EVENT_TYPES_ONB_PROCESSSTATE',
    'ADS_RDS_EVENT_TYPES_CONDITIONS',
    'ADS_RDS_EVENT_TYPES_MEP'
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def sanitize_filename(filename):
    """
    Remove or replace characters that are not valid in filenames
    """
    # Replace invalid characters with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove any trailing dots or spaces
    sanitized = sanitized.strip('. ')
    return sanitized

def build_mapping_in_clause(mapping_list):
    """
    Build the IN clause for the SQL query from the mapping list
    """
    quoted_mappings = [f"'{mapping}'" for mapping in mapping_list]
    return ',\n'.join(quoted_mappings)

def export_etl_mappings_to_files(output_directory="./IMP_Map_Ora"):
    """
    Export ETL mappings from Oracle database to individual SQL files
    
    Args:
        output_directory: Directory where SQL files will be created
    """
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    
    connection = None
    cursor = None
    
    try:
        # Initialize Oracle client in thick mode (required for network encryption)
        try:
            oracledb.init_oracle_client()
            print("Oracle client initialized in thick mode")
        except Exception:
            print("Note: Thick mode initialization failed, using thin mode")
        
        # Connect to Oracle database
        connection = oracledb.connect(
            user="ext98174",
            password="Cervenec2025**",
            dsn="pr03db-scan.vs.csin.cz:1521/DWHP"
        )
        
        print("Successfully connected to Oracle database!")
        cursor = connection.cursor()
        
        # Build the IN clause from the mapping list
        mapping_in_clause = build_mapping_in_clause(MAPPING_LIST)
        print(f"Processing {len(MAPPING_LIST)} mappings...")
        
        # Your ETL mappings query with dynamic mapping list
        query = f"""
WITH 
map_list as (
 select * from
   ads_lib_etl_owner.etl_mappings map
 where map.map_name in
(
{mapping_in_clause}
)
),
-- Get primary key columns for each target table
pk_columns AS (
    SELECT 
        acc.owner,
        acc.table_name,
        LISTAGG(LOWER(acc.column_name), '||''.''||') WITHIN GROUP (ORDER BY acc.position) as pk_concat_expr,
        LISTAGG(LOWER(acc.column_name), ', ') WITHIN GROUP (ORDER BY acc.position) as pk_columns_list
    FROM all_cons_columns acc
    JOIN all_constraints ac ON acc.owner = ac.owner 
                           AND acc.constraint_name = ac.constraint_name
    WHERE ac.constraint_type = 'P'
      AND acc.owner IN (SELECT DISTINCT TRG_TABLE_OWNER FROM ads_lib_etl_owner.etl_mappings)
    GROUP BY acc.owner, acc.table_name
),
X AS (
    SELECT
        log_name,
        TRG_TABLE_OWNER,TRG_TABLE_NAME
        ,MAX(log_key) as last_log_key
        ,MAX('to_date('''''||to_char(log_effective_date,'yyyymmdd')||''''',''''yyyymmdd'''')') as effdate
        ,MAX('to_date('''''||to_char(log_effective_date,'YYYYMMDD')||''''',''''YYYYMMDD'''')') as effdate_upper
        ,MAX(log_params) as p_param
        ,MAX(log_process_key) as p_process_key
    FROM ads_lib_etl_owner.ETL_TASK_LOGS l 
    JOIN map_list ml
      ON map_name = log_name
    WHERE (log_name, log_effective_date) IN ( SELECT log_name, MAX(log_effective_date) FROM ads_lib_etl_owner.ETL_TASK_LOGS l WHERE log_status = 'OK' 
            GROUP BY log_name
            )
        AND log_status = 'OK'
    GROUP BY log_name,TRG_TABLE_OWNER,TRG_TABLE_NAME
),
cmds AS(
SELECT /*+ ORDERED */
  LOG_EFFECTIVE_DATE,
  log_map_id,
  TRG_TABLE_OWNER,TRG_TABLE_NAME,
  LOGDET_STEP_ID,
  LOG_START_DATETIME,
  LOG_END_DATETIME,
  ld.logdet_key,
  pk.pk_concat_expr,
  case when logdet_step_id = 'LOG_MAP_SQL' 
            then '/*'||'LOAD_STAGE'||'*/'||chr(10)||'INSERT INTO ads_etl_owner.XC_'||substr(log_map_id,5)||' ' || CHR(10)||CHR(10) || 
                 -- Replace Oracle ROWID with concatenated primary key
                 REPLACE(
                   REPLACE(
                     REPLACE(logcmd_text, 
                             't.rowid as rid,', 
                             CASE WHEN pk.pk_concat_expr IS NOT NULL 
                                  THEN pk.pk_concat_expr || ' as rid,'
                                  ELSE 'est_key as rid,'
                             END),
                     'rowid as rid,',
                     CASE WHEN pk.pk_concat_expr IS NOT NULL 
                          THEN pk.pk_concat_expr || ' as rid,'
                          ELSE 'est_key as rid,'
                     END),
                   'trg.rid as tech$rid',
                   'trg.rid as tech_rid'
                 )
            else 
                 -- Replace Oracle ROWID in other steps as well
                 '/*'||logdet_step_id||'*/'||chr(10)||REPLACE(
                   REPLACE(
                     REPLACE(logcmd_text, 
                             't.rowid as rid,', 
                             CASE WHEN pk.pk_concat_expr IS NOT NULL 
                                  THEN pk.pk_concat_expr || ' as rid,'
                                  ELSE 'est_key as rid,'
                             END),
                     'rowid as rid,',
                     CASE WHEN pk.pk_concat_expr IS NOT NULL 
                          THEN pk.pk_concat_expr || ' as rid,'
                          ELSE 'est_key as rid,'
                     END),
                   'trg.rid as tech$rid',
                   'trg.rid as tech_rid'
                 )
       end SQL_cmd
FROM X x
    JOIN ads_lib_etl_owner.ETL_TASK_LOGS l
        ON l.log_key = x.last_log_key
    JOIN ads_lib_etl_owner.ETL_TASK_LOG_DETAILS ld
        ON  l.log_key = ld.log_key
            AND ld.logdet_step_id NOT IN ('LOG_METADATA_MAPPING','LOG_METADATA_COLUMNS','STAGE','STATISTIC','REBUILD')
    JOIN ads_lib_etl_owner.ETL_TASK_LOG_CMDS lc
        ON  ld.logdet_key = lc.logdet_key
    LEFT JOIN pk_columns pk
        ON pk.owner = x.TRG_TABLE_OWNER
        AND pk.table_name = x.TRG_TABLE_NAME
WHERE 1=1
)
select 
log_map_id map_id,
-- Additional replacements for Databricks compatibility including VARCHAR2 to VARCHAR conversion
REPLACE(
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      LISTAGG(SQL_cmd,';'||chr(10)) WITHIN GROUP (ORDER BY logdet_key),
                      'tech$', 'tech_'
                    ),
                    'ora$ptt_ads_map_scd_diff', 'diff_'||log_map_id||'_ads_map_scd_diff'
                  ),
                  'private temporary', ''
                ),
                'on commit drop definition', ''
              ),
              'to_date(''30000101'',''YYYYMMDD'')', 'to_date(''30000101'',''yyyyMMdd'')'
            ),
            'and rowid in (select tech_rid from', 
            'and ' || COALESCE(MAX(pk_concat_expr), 'est_key') || ' in (select tech_rid from'
          ),
          'rowid', COALESCE(MAX(pk_concat_expr), 'est_key')
        ),
        'to_date(''01013000'',''ddmmyyyy'')', 'to_date(''01013000'',''ddMMyyyy'')'
      ),
      'VARCHAR2', 'varchar'  -- Add this for uppercase
    ),
    'varchar2', 'varchar'    -- Keep this for lowercase
  ),
  ' char)', ')'
) cmd,
TRG_TABLE_OWNER,TRG_TABLE_NAME
from cmds
group by log_map_id,TRG_TABLE_OWNER,TRG_TABLE_NAME
order by LOG_MAP_ID
        """
        
        print("Executing ETL mappings query...")
        cursor.execute(query)
        
        results = cursor.fetchall()
        print(f"Found {len(results)} mappings to export")
        
        file_count = 0
        
        # Process each row
        for row in results:
            mapping_name = row[0]  # MAP_ID
            cmd = row[1]          # CMD
            
            if mapping_name and cmd:
                safe_filename = sanitize_filename(mapping_name)
                file_path = os.path.join(output_directory, f"{safe_filename}.sql")
                
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- ETL Mapping: {mapping_name}\n")
                    file.write(f"-- Generated from DWHP logs (ads_lib_etl_owner.etl_task_logs)\n")
                    file.write(f"-- Export date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    # Handle CLOB data if needed
                    if hasattr(cmd, 'read'):
                        cmd_text = cmd.read()
                    else:
                        cmd_text = str(cmd)
                    
                    file.write(cmd_text)
                    if not cmd_text.endswith('\n'):
                        file.write('\n')
                
                file_count += 1
                print(f"Created: {file_path}")
        
        print(f"\nExport completed successfully!")
        print(f"{file_count} SQL files created in '{output_directory}'")
        print(f"Output directory: {os.path.abspath(output_directory)}")
        
    except oracledb.Error as e:
        print(f"Oracle database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up resources properly
        if cursor:
            try:
                cursor.close()
            except:
                pass  # Cursor might already be closed
        if connection:
            try:
                connection.close()
                print("Database connection closed.")
            except:
                pass  # Connection might already be closed

def main():
    """
    Main function to run the export
    """
    print("ETL Oracle Mappings Export Tool")
    print("=" * 50)
    print(f"Processing {len(MAPPING_LIST)} ETL mappings:")
    for i, mapping in enumerate(MAPPING_LIST, 1):
        print(f"  {i:2d}. {mapping}")
    print("=" * 50)
    
    export_etl_mappings_to_files(output_dir)

if __name__ == "__main__":
    main()