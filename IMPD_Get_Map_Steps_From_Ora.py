#!/usr/bin/env python3
"""
Databricks version of Oracle ETL Mappings Export Script
Exports ETL mapping SQL commands to individual files in DBFS
Uses Spark JDBC connection to Oracle DWHP database
"""

import re
from datetime import datetime
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# Oracle connection configuration (same as Table_Import_Ora2DBX.py)
oracle_config = {
    "jdbc_url": "jdbc:oracle:thin:@//pr03db-scan.vs.csin.cz:1521/DWHP",
    "user": "ext98174",
    "password": "Cervenec2025**",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Output directory in DBFS for generated SQL files
output_dir = "/dbfs/FileStore/etl_mappings/IMP_Map_Ora"

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

def ensure_dbfs_directory(directory_path):
    """
    Ensure DBFS directory exists
    """
    try:
        dbutils.fs.mkdirs(directory_path.replace('/dbfs', ''))
        return True
    except Exception as e:
        print(f"Warning: Could not create directory {directory_path}: {e}")
        return False

def write_file_to_dbfs(file_path, content):
    """
    Write content to DBFS file
    """
    try:
        # Convert /dbfs path to dbfs: path for dbutils
        dbfs_path = file_path.replace('/dbfs', 'dbfs:')
        
        # Write content using dbutils
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Created: {dbfs_path}")
        return True
    except Exception as e:
        print(f"❌ Error writing file {file_path}: {e}")
        return False

def export_etl_mappings_to_files_databricks(output_directory=output_dir):
    """
    Export ETL mappings from Oracle database to individual SQL files using Spark JDBC
    
    Args:
        output_directory: Directory where SQL files will be created (DBFS path)
    """
    
    # Ensure output directory exists
    ensure_dbfs_directory(output_directory)
    
    try:
        # Build the IN clause from the mapping list
        mapping_in_clause = build_mapping_in_clause(MAPPING_LIST)
        print(f"🔄 Processing {len(MAPPING_LIST)} mappings...")
        
        # Complex ETL mappings query adapted for Spark SQL
        query_template = """
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
log_map_id as MAP_ID,
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
) as CMD,
TRG_TABLE_OWNER,TRG_TABLE_NAME
from cmds
group by log_map_id,TRG_TABLE_OWNER,TRG_TABLE_NAME
order by LOG_MAP_ID
        """
        
        # Format the query with the mapping list
        formatted_query = query_template.format(mapping_in_clause=mapping_in_clause)
        
        print("🔍 Executing ETL mappings query using Spark JDBC...")
        
        # Execute query using Spark JDBC (similar to Table_Import_Ora2DBX.py approach)
        query_wrapper = f"({formatted_query}) etl_mappings_query"
        
        results_df = spark.read \
            .format("jdbc") \
            .option("url", oracle_config["jdbc_url"]) \
            .option("dbtable", query_wrapper) \
            .option("user", oracle_config["user"]) \
            .option("password", oracle_config["password"]) \
            .option("driver", oracle_config["driver"]) \
            .option("fetchsize", "1000") \
            .load()
        
        # Collect results
        results = results_df.collect()
        
        print(f"✅ Found {len(results)} mappings to export")
        
        if len(results) == 0:
            print("⚠️  No mappings found. Check if the mapping names exist in the database.")
            return
        
        file_count = 0
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Process each row
        for row in results:
            mapping_name = row['MAP_ID']
            cmd = row['CMD']
            trg_table_owner = row['TRG_TABLE_OWNER']
            trg_table_name = row['TRG_TABLE_NAME']
            
            if mapping_name and cmd:
                safe_filename = sanitize_filename(mapping_name)
                file_path = f"{output_directory}/{safe_filename}.sql"
                
                # Prepare file content
                file_content = f"""-- ETL Mapping: {mapping_name}
-- Target: {trg_table_owner}.{trg_table_name}
-- Generated from DWHP logs (ads_lib_etl_owner.etl_task_logs)
-- Export date: {current_time}
-- Exported via Databricks Spark JDBC

{cmd}
"""
                
                # Write file to DBFS
                if write_file_to_dbfs(file_path, file_content):
                    file_count += 1
        
        print(f"\n🎉 Export completed successfully!")
        print(f"📁 {file_count} SQL files created in '{output_directory}'")
        print(f"🔗 Files available in DBFS at: {output_directory.replace('/dbfs', 'dbfs:')}")
        
        # Display summary information
        print(f"\n📊 Export Summary:")
        print(f"   • Total mappings processed: {len(results)}")
        print(f"   • Files created: {file_count}")
        print(f"   • Output directory: {output_directory}")
        
        # Show first few results as sample
        if len(results) > 0:
            print(f"\n📋 Sample mappings exported:")
            for i, row in enumerate(results[:5]):
                print(f"   {i+1}. {row['MAP_ID']}")
            if len(results) > 5:
                print(f"   ... and {len(results) - 5} more")
        
    except Exception as e:
        print(f"❌ Error during export: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

def list_exported_files(output_directory=output_dir):
    """
    List all exported SQL files
    """
    try:
        dbfs_path = output_directory.replace('/dbfs', 'dbfs:')
        files = dbutils.fs.ls(dbfs_path)
        
        print(f"📁 Files in {dbfs_path}:")
        sql_files = [f for f in files if f.name.endswith('.sql')]
        
        for i, file_info in enumerate(sql_files, 1):
            print(f"   {i:2d}. {file_info.name} ({file_info.size} bytes)")
        
        print(f"\nTotal: {len(sql_files)} SQL files")
        return sql_files
        
    except Exception as e:
        print(f"❌ Error listing files: {e}")
        return []

def display_file_content(filename, output_directory=output_dir, lines=20):
    """
    Display first N lines of a specific exported file
    """
    try:
        file_path = f"{output_directory}/{filename}"
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content_lines = f.readlines()
        
        print(f"📄 First {min(lines, len(content_lines))} lines of {filename}:")
        print("="*60)
        
        for i, line in enumerate(content_lines[:lines], 1):
            print(f"{i:3d}: {line.rstrip()}")
        
        if len(content_lines) > lines:
            print(f"... ({len(content_lines) - lines} more lines)")
        
        print("="*60)
        print(f"Total lines: {len(content_lines)}")
        
    except Exception as e:
        print(f"❌ Error displaying file {filename}: {e}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main function to run the export
    """
    print("🚀 ETL Oracle Mappings Export Tool - Databricks Version")
    print("=" * 70)
    print(f"📋 Processing {len(MAPPING_LIST)} ETL mappings:")
    
    # Display mapping list in columns for better readability
    for i in range(0, len(MAPPING_LIST), 2):
        left = f"  {i+1:2d}. {MAPPING_LIST[i]}"
        right = f"  {i+2:2d}. {MAPPING_LIST[i+1]}" if i+1 < len(MAPPING_LIST) else ""
        print(f"{left:<50} {right}")
    
    print("=" * 70)
    print(f"🎯 Output directory: {output_dir}")
    print(f"🔗 Oracle connection: {oracle_config['jdbc_url']}")
    print("=" * 70)
    
    # Run the export
    export_etl_mappings_to_files_databricks(output_dir)

# =============================================================================
# UTILITY FUNCTIONS FOR INTERACTIVE USE
# =============================================================================

def quick_export():
    """Quick export with default settings"""
    export_etl_mappings_to_files_databricks()

def show_files():
    """Show list of exported files"""
    return list_exported_files()

def show_file(filename, lines=20):
    """Show content of a specific file"""
    display_file_content(filename, lines=lines)

def test_connection():
    """Test Oracle connection"""
    try:
        print("🔍 Testing Oracle connection...")
        test_df = spark.read \
            .format("jdbc") \
            .option("url", oracle_config["jdbc_url"]) \
            .option("dbtable", "(SELECT 'Connection successful!' as test_result FROM dual) test_query") \
            .option("user", oracle_config["user"]) \
            .option("password", oracle_config["password"]) \
            .option("driver", oracle_config["driver"]) \
            .load()
        
        result = test_df.collect()[0]['test_result']
        print(f"✅ {result}")
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

# Run main function if executed directly
if __name__ == "__main__":
    main()

# =============================================================================
# QUICK START EXAMPLES
# =============================================================================

print("\n" + "="*70)
print("🎯 QUICK START EXAMPLES:")
print("="*70)
print("# Test Oracle connection:")
print("test_connection()")
print()
print("# Run export with all mappings:")
print("main()")
print()
print("# Quick export (same as main but shorter):")
print("quick_export()")
print()
print("# List exported files:")
print("show_files()")
print()
print("# Show content of specific file:")
print('show_file("ADS_SMA_PROCESS_EVENTS_MAIN.sql", lines=30)')
print("="*70)
