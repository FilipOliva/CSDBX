"""
Oracle to Databricks SCD2 Pattern Converter - FIXED VERSION
Converts Oracle SCD2 ETL scripts to Databricks notebooks or SQL scripts
Fixed issue with missing column lists in notebook INSERT statements
"""

import re
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class OracleToDatabricsConverter:
    def __init__(self):
        self.schema_mappings = {
            'ads_etl_owner': 'gap_catalog.ads_etl_owner',
            'ADS_ETL_OWNER': 'gap_catalog.ads_etl_owner', 
            'ads_owner': 'gap_catalog.ads_owner',
            'ADS_OWNER': 'gap_catalog.ads_owner'
        }
        
        self.date_mappings = {
            'sysdate': 'CURRENT_TIMESTAMP()',
            'SYSDATE': 'CURRENT_TIMESTAMP()'
        }
    
    def normalize_sql_ending(self, sql: str) -> str:
        """Ensure SQL ends with exactly one semicolon"""
        sql = sql.strip()
        # Remove all trailing semicolons
        while sql.endswith(';'):
            sql = sql[:-1].strip()
        # Add exactly one semicolon
        return sql + ';'
    
    def convert_oracle_joins(self, sql_text):
        """
        Convert Oracle outer join syntax (+) to ANSI LEFT JOIN syntax.
        Handles STEP 4 and STEP 5 patterns from SCD2 loading process.
        """
        
        # Pattern 1: STEP 4 - trg.column (+) = src.column -> src LEFT JOIN trg
        step4_pattern = (
            r'(\s+from\s+)\(\s*(select\s+.*?)\)\s*src\s*,\s*'
            r'\(\s*(select\s+.*?)\)\s*trg\s+'
            r'where\s+(trg\.\w+\s*\(\+\)\s*=\s*src\.\w+.*?)'
            r'(\s+and\s+trg\.EST_VALID_TO\s*\(\+\)\s*=\s*[^)]+\))?'
            r'(\s+and\s+\(\s*decode.*?(?:\s+or\s+trg\.\w+\s+is\s+null\s*)?\s*\))'
        )
        
        def replace_step4(match):
            indent = match.group(1)
            src_query = match.group(2)
            trg_query = match.group(3)
            join_conditions = match.group(4)
            valid_to_condition = match.group(5) or ""
            where_clause = match.group(6)
            
            # Clean join conditions
            clean_conditions = re.sub(r'\s*\(\+\)', '', join_conditions)
            clean_conditions = re.sub(r'where\s+', '', clean_conditions, flags=re.IGNORECASE)
            
            # Add valid_to condition if present
            if valid_to_condition:
                clean_valid_to = re.sub(r'\s*\(\+\)', '', valid_to_condition)
                clean_valid_to = re.sub(r'\s+and\s+', '', clean_valid_to, flags=re.IGNORECASE)
                clean_conditions += f"\n and {clean_valid_to.strip()}"
            
            # Format the result
            result = (
                f"{indent}({src_query}) src LEFT JOIN\n"
                f"    ({trg_query}) trg\n"
                f"ON {clean_conditions.strip()}"
                f"{where_clause.replace(' and (', ' WHERE (')}"
            )
            
            return result
        
        sql_text = re.sub(step4_pattern, replace_step4, sql_text, flags=re.IGNORECASE | re.DOTALL)
        
        # Pattern 2: STEP 5 - trg.column = src.column (+) -> trg LEFT JOIN src
        step5_pattern = (
            r'(\s+from\s+)\(\s*(select\s+.*?)\)\s*src\s*,\s*'
            r'\(\s*(select\s+.*?)\)\s*trg\s+'
            r'where\s+(trg\.\w+\s*=\s*src\.\w+\s*\(\+\).*?)'
            r'(\s+and\s+trg\.EST_VALID_TO\s*=\s*src\.EST_VALID_TO\s*\(\+\))?'
            r'(\s+and\s+\(\s*src\.\w+\s+is\s+null\s*\))'
        )
        
        def replace_step5(match):
            indent = match.group(1)
            src_query = match.group(2)
            trg_query = match.group(3)
            join_conditions = match.group(4)
            valid_to_condition = match.group(5) or ""
            where_clause = match.group(6)
            
            # Clean join conditions
            clean_conditions = re.sub(r'\s*\(\+\)', '', join_conditions)
            clean_conditions = re.sub(r'where\s+', '', clean_conditions, flags=re.IGNORECASE)
            
            # Add valid_to condition if present
            if valid_to_condition:
                clean_valid_to = re.sub(r'\s*\(\+\)', '', valid_to_condition)
                clean_valid_to = re.sub(r'\s+and\s+', '', clean_valid_to, flags=re.IGNORECASE)
                clean_conditions += f"\n and {clean_valid_to.strip()}"
            
            # Format the result (note: trg LEFT JOIN src for this pattern)
            result = (
                f"{indent}({trg_query}) trg LEFT JOIN\n"
                f"    ({src_query}) src\n"
                f"ON {clean_conditions.strip()}"
                f"{where_clause.replace(' and (', ' WHERE (')}"
            )
            
            return result
        
        sql_text = re.sub(step5_pattern, replace_step5, sql_text, flags=re.IGNORECASE | re.DOTALL)
        
        return sql_text
        
    def extract_mapping_info(self, sql_content: str) -> Dict:
        """Extract key information from Oracle SQL"""
        info = {}
        
        # Extract mapping name from header comment
        mapping_match = re.search(r'-- ETL Mapping: (\w+)', sql_content)
        info['mapping_name'] = mapping_match.group(1) if mapping_match else 'UNKNOWN_MAPPING'
        
        # Extract XC table name
        xc_match = re.search(r'INSERT INTO\s+\w*\.?(XC_\w+)', sql_content, re.IGNORECASE)
        if xc_match:
            info['xc_table'] = xc_match.group(1)
        else:
            xc_match = re.search(r'INSERT INTO\s+\w*\.?(\w+)', sql_content, re.IGNORECASE)
            info['xc_table'] = xc_match.group(1) if xc_match else 'XC_TABLE'
        
        # Extract diff table name - handle multiple spaces in "create  table"
        diff_table_match = re.search(r'create\s+table\s+(diff_\w+)', sql_content, re.IGNORECASE)
        if diff_table_match:
            info['diff_table_oracle'] = diff_table_match.group(1)
            info['diff_table_name'] = f"gap_catalog.ads_owner.{diff_table_match.group(1).upper()}"
        else:
            info['diff_table_oracle'] = f"diff_{info['mapping_name']}_ads_map_scd_diff"
            info['diff_table_name'] = f"gap_catalog.ads_owner.DIFF_{info['mapping_name']}"
        
        # Extract target table info from diff table definition
        diff_table_match = re.search(r'create\s+table\s+\w+.*?(\w+_KEY)\s+INTEGER', 
                                   sql_content, re.DOTALL | re.IGNORECASE)
        if diff_table_match:
            key_column = diff_table_match.group(1)
            if key_column == 'EST_KEY':
                info['target_table'] = 'event_status'
            elif key_column == 'EVETP_KEY':
                info['target_table'] = 'event_types'
            else:
                table_prefix = key_column.replace('_KEY', '').lower()
                info['target_table'] = table_prefix
            info['key_column'] = key_column
        
        # Extract all columns from diff table definition  
        diff_table_section = re.search(r'create\s+table\s+\w+\s*\((.*?)\)\s*', 
                                     sql_content, re.DOTALL | re.IGNORECASE)
        if diff_table_section:
            columns_text = diff_table_section.group(1)
            info['columns'] = self.parse_table_columns(columns_text)
        
        # Extract date parameter
        date_match = re.search(r"to_date\('(\d{8})'", sql_content)
        info['load_date'] = date_match.group(1) if date_match else '20250827'
        
        # Extract process key
        process_match = re.search(r'(\d+) as \w*_INSERT_PROCESS_KEY', sql_content)
        info['process_key'] = process_match.group(1) if process_match else '-999'
        
        return info
    
    def parse_table_columns(self, columns_text: str) -> List[str]:
        """Parse column definitions from table"""
        columns = []
        lines = columns_text.strip().split(',')
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--'):
                col_match = re.match(r'(\w+)', line)
                if col_match:
                    columns.append(col_match.group(1))
        return columns
    
    def convert_column_names(self, sql: str) -> str:
        """Convert Oracle column names with $ to underscores"""
        sql = re.sub(r'tech\$del_flg', 'tech_del_flg', sql)
        sql = re.sub(r'tech\$new_rec', 'tech_new_rec', sql)
        sql = re.sub(r'tech\$rid', 'tech_rid', sql)
        return sql
    
    def convert_schema_names(self, sql: str) -> str:
        """Convert Oracle schema names to Databricks catalog.schema format"""
        for oracle_schema, databricks_schema in self.schema_mappings.items():
            pattern = f'\\b{oracle_schema}\\.'
            if f'gap_catalog.{oracle_schema.lower()}' not in sql:
                sql = re.sub(pattern, f'{databricks_schema}.', sql, flags=re.IGNORECASE)
        return sql
    
    def convert_date_functions(self, sql: str) -> str:
        """Convert Oracle date functions to Spark SQL equivalents"""
        for oracle_func, spark_func in self.date_mappings.items():
            sql = sql.replace(oracle_func, spark_func)
        return sql
    
    def extract_select_columns(self, sql: str) -> List[str]:
        """Extract column names from SELECT statement after FROM clause"""
        # Find the SELECT statement with columns
        select_match = re.search(r'select\s+(.*?)\s+from\s+', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []
        
        select_clause = select_match.group(1)
        
        # Remove comments and clean up
        select_clause = re.sub(r'/\*.*?\*/', '', select_clause, flags=re.DOTALL)
        select_clause = re.sub(r'--.*$', '', select_clause, flags=re.MULTILINE)
        
        # Split by comma and extract column names or aliases
        columns = []
        column_parts = select_clause.split(',')
        
        for part in column_parts:
            part = part.strip()
            if not part:
                continue
                
            # Look for AS alias first
            as_match = re.search(r'\s+as\s+(\w+)\s*$', part, re.IGNORECASE)
            if as_match:
                columns.append(as_match.group(1))
                continue
            
            # Look for simple alias (space-separated)
            alias_match = re.search(r'(\w+)\s*$', part)
            if alias_match:
                column_name = alias_match.group(1)
                # Skip if it's likely part of a function call
                if not re.search(r'\w+\s*\(', part):
                    columns.append(column_name)
        
        return columns
    
    def add_insert_column_list(self, sql: str, info: Dict) -> str:
        """Add explicit column list to INSERT statements, extracting from SELECT clause"""
        
        # Extract columns from the SELECT statement
        select_columns = self.extract_select_columns(sql)
        
        if not select_columns:
            # Fallback to default column list if extraction fails
            key_col = info.get('key_column', 'EST_KEY')
            select_columns = [
                'tech_del_flg',
                'tech_new_rec', 
                'tech_rid',
                key_col,
                key_col.replace('_KEY', '_SOURCE_ID'),
                key_col.replace('_KEY', '_SOURCE_SYSTEM_ID'),
                key_col.replace('_KEY', '_SOURCE_SYS_ORIGIN'),
                key_col.replace('_KEY', '_DESC')
            ]
        
        # Remove auto-generated identity columns from INSERT column list
        key_col = info.get('key_column', 'EST_KEY')
        auto_gen_col = f"{key_col}_NEW"
        
        insert_columns = [col for col in select_columns if col != auto_gen_col]
        
        # Format column list
        formatted_columns = ',\n'.join([f'  {col}' for col in insert_columns])
        column_clause = f"(\n{formatted_columns}\n)"
        
        # Find INSERT INTO statement and add column list
        pattern = r'(insert\s+into\s+[\w.${}]+)\s*(select)'
        replacement = f'\\1\n{column_clause}\n\\2'
        
        return re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    def extract_sql_sections(self, sql_content: str) -> Dict[str, str]:
        """Extract different sections from Oracle SQL using comment markers"""
        sections = {}
        
        # Define the mapping from comment markers to section keys
        comment_mappings = {
            '/*LOAD_STAGE*/': 'xc_insert',
            '/*CREATE_TEMP*/': 'diff_table_create', 
            '/*PREPARE_DIFF*/': 'diff_insert_new',
            '/*PREPARE_DELETED*/': 'diff_insert_deleted',
            '/*CLOSE_OLD*/': 'target_update',
            '/*INSERT_CHANGED*/': 'target_insert_changed',
            '/*INSERT_NEW*/': 'target_insert_new',
            '/*DROP_TEMP*/': 'drop_table'
        }
        
        # Split content by comment markers
        current_section = None
        current_content = []
        
        lines = sql_content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            
            # Check if this line contains a comment marker
            found_marker = None
            for marker, section_key in comment_mappings.items():
                if marker in line_stripped:
                    found_marker = section_key
                    break
            
            if found_marker:
                # Save previous section if we have content
                if current_section and current_content:
                    content = '\n'.join(current_content).strip()
                    if content:
                        sections[current_section] = content
                
                # Start new section
                current_section = found_marker
                current_content = []
            elif current_section:
                # Add line to current section (skip comment lines and empty lines for cleaner content)
                if line_stripped and not line_stripped.startswith('--'):
                    current_content.append(line)
        
        # Don't forget the last section
        if current_section and current_content:
            content = '\n'.join(current_content).strip()
            if content:
                sections[current_section] = content
                    
        return sections

    def generate_databricks_notebook(self, sql_content: str, p_load_date: str = '2025-08-31') -> str:
        """Generate complete Databricks notebook from Oracle SQL with proper parameter handling"""
        info = self.extract_mapping_info(sql_content)
        sections = self.extract_sql_sections(sql_content)
        
        notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC ## Mapping steps {info['mapping_name']}
# MAGIC - Generated from Oracle Import file
# MAGIC - Export date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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

dbutils.widgets.text("p_load_date", "{p_load_date}", "Load Date")
dbutils.widgets.text("p_process_key", "{info['process_key']}", "Process Key")

map_id = '{info['mapping_name']}'
schema_name = 'gap_catalog.ads_owner'
dif_table = '{info['diff_table_name']}'

# Get the maximum {info['key_column']} from Target table
max_key_result = spark.sql("""
    SELECT COALESCE(MAX({info['key_column'].lower()}), 0) as max_key 
    FROM gap_catalog.ads_owner.{info['target_table']}
""").collect()

max_key = max_key_result[0]['max_key']
print(f"Current maximum {info['key_column']}: {{max_key}}")

spark.sql(f"SET var.dif_table_name = {{dif_table}}")
spark.conf.set("var.max_key", str(max_key))
p_load_date = dbutils.widgets.get("p_load_date")
print("p_load_date: "+p_load_date)

# COMMAND ----------

# DBTITLE 1,Truncate XC Table
# MAGIC %sql truncate table {self.schema_mappings['ADS_ETL_OWNER']}.{info['xc_table']}

# COMMAND ----------

# DBTITLE 1,Fill XC Table
# MAGIC %sql
{self.convert_xc_insert_section_notebook(sections.get('xc_insert', ''), info)}

# COMMAND ----------

# DBTITLE 1,Cleanup DIFF Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS {info['diff_table_name']};

# COMMAND ----------

# DBTITLE 1,Create DIFF Table
# MAGIC %sql
{self.convert_diff_table_creation_notebook(sections.get('diff_table_create', ''), info)}

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - New/Updated Records
# MAGIC %sql
{self.convert_diff_insert_new_section_notebook(sections.get('diff_insert_new', ''), info)}

# COMMAND ----------

# DBTITLE 1,Populate DIFF Table - Deleted Records
# MAGIC %sql
{self.convert_diff_insert_deleted_section_notebook(sections.get('diff_insert_deleted', ''), info)}

# COMMAND ----------

# DBTITLE 1,Close Old Records in Target
# MAGIC %sql
{self.convert_target_update_section_notebook(sections.get('target_update', ''), info)}

# COMMAND ----------

# DBTITLE 1,Insert Changed Records
# MAGIC %sql
{self.convert_target_insert_changed_section_notebook(sections.get('target_insert_changed', ''), info)}

# COMMAND ----------

# DBTITLE 1,Insert New Records
# MAGIC %sql
{self.convert_target_insert_new_section_notebook(sections.get('target_insert_new', ''), info)}

# COMMAND ----------

# DBTITLE 1,Validation - Row Counts
# MAGIC %sql 
{self.generate_validation_section_notebook(info)}
'''
        return notebook_content
    
    # NOTEBOOK-SPECIFIC CONVERSION METHODS (using : notation) - FIXED VERSIONS
    def convert_xc_insert_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert XC table insert section for notebook (using : notation)"""
        if not sql:
            return "-- XC insert section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        sql = re.sub(r"to_date\('\d{8}','yyyymmdd'\)", "DATE(':p_load_date')", sql)
        sql = re.sub(r"sys_effective_date = to_date\('\d{8}','yyyymmdd'\)", 
                    "DATE(from_utc_timestamp(SYS_EFFECTIVE_DATE, 'Europe/Prague')) = :p_load_date", sql)
        
        return sql
    
    def convert_diff_table_creation_notebook(self, sql: str, info: Dict) -> str:
        """Convert DIFF table creation for notebook"""
        if not sql:
            # Generate a default DIFF table if not found
            databricks_table_name = info.get('diff_table_name', f"gap_catalog.ads_owner.DIFF_{info.get('mapping_name', 'UNKNOWN')}")
            key_col = info.get('key_column', 'EST_KEY')
            
            return f"""CREATE TABLE {databricks_table_name} (
  tech_del_flg  CHAR(1),
  tech_new_rec  CHAR(1),
  tech_rid      VARCHAR(655),
  {key_col}  INTEGER,
  {key_col}_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${{var.max_key}} INCREMENT BY 1),
  {key_col.replace('_KEY', '_SOURCE_ID')}  VARCHAR(120),
  {key_col.replace('_KEY', '_SOURCE_SYSTEM_ID')}  VARCHAR(120),
  {key_col.replace('_KEY', '_SOURCE_SYS_ORIGIN')}  VARCHAR(120),
  {key_col.replace('_KEY', '_DESC')}  VARCHAR(4000)
);
"""
            
        # Convert existing CREATE TABLE
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        
        # Replace Oracle table name with Databricks name
        oracle_table_name = info.get('diff_table_oracle', 'diff_table')
        databricks_table_name = info.get('diff_table_name', 'gap_catalog.ads_owner.diff_table')
        sql = sql.replace(oracle_table_name, databricks_table_name)
        
        # Add auto-increment column with variable reference
        key_col = info.get('key_column', 'EST_KEY')
        if f"{key_col}_NEW" not in sql:
            pattern = f"({key_col}\\s+INTEGER)"
            replacement = f"\\1,\n  {key_col}_NEW BIGINT GENERATED ALWAYS AS IDENTITY (START WITH ${{var.max_key}} INCREMENT BY 1)"
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        
        return sql
    
    def convert_diff_insert_new_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert DIFF table insert for new/updated records - notebook version - FIXED"""
        if not sql:
            return "-- DIFF insert new section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        sql = sql.replace(info['diff_table_oracle'], '${var.dif_table_name}')
        
        # FIX: Add explicit column list to INSERT statement (excluding auto-generated columns)
        sql = self.add_insert_column_list(sql, info)
        
        return sql
    
    def convert_diff_insert_deleted_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert DIFF table insert for deleted records - notebook version - FIXED"""
        if not sql:
            return "-- DIFF insert deleted section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        sql = sql.replace(info['diff_table_oracle'], '${var.dif_table_name}')
        
        # FIX: Add explicit column list to INSERT statement (excluding auto-generated columns)
        sql = self.add_insert_column_list(sql, info)
        
        return sql
    
    def convert_target_update_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert target table update section - notebook version"""
        if not sql:
            return "-- Target update section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        
        sql = sql.replace(info['diff_table_oracle'], '${var.dif_table_name}')
        sql = re.sub(r'\b\d{7,8}\b(?=\s+as\s+\w+_UPDATE_PROCESS_KEY|\s*,?\s*$)', ':p_process_key', sql, flags=re.IGNORECASE)
        sql = re.sub(r"EST_VALID_TO\s*=\s*to_date\('(\d{8})','YYYYMMDD'\)\s*-\s*1", 
                     "EST_VALID_TO = to_date(':p_load_date','yyyy-MM-dd')-1", sql, flags=re.IGNORECASE)
        
        return sql

    def convert_target_insert_changed_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert target table insert for changed records - notebook version"""
        if not sql:
            return "-- Target insert changed section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        sql = sql.replace(info['diff_table_oracle'], '${var.dif_table_name}')
        
        # Replace process key numbers specifically
        sql = re.sub(r'\b\d{7,8}\b(?=\s+as\s+\w+_INSERT_PROCESS_KEY)', ':p_process_key', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\b\d{7,8}\b(?=\s+as\s+\w+_UPDATE_PROCESS_KEY)', ':p_process_key', sql, flags=re.IGNORECASE)
        
        # Convert load date patterns for EST_VALID_FROM
        sql = re.sub(r"to_date\('(\d{8})','YYYYMMDD'\)\s+as\s+EST_VALID_FROM", 
                     "to_date(':p_load_date','yyyy-MM-dd') as EST_VALID_FROM", sql, flags=re.IGNORECASE)
        
        return sql

    def convert_target_insert_new_section_notebook(self, sql: str, info: Dict) -> str:
        """Convert target table insert for new records - notebook version"""
        if not sql:
            return "-- Target insert new section not found"
            
        sql = self.convert_schema_names(sql)
        sql = self.convert_column_names(sql)
        sql = self.convert_date_functions(sql)
        sql = self.convert_oracle_joins(sql)
        sql = sql.replace(info['diff_table_oracle'], '${var.dif_table_name}')
        
        # Replace Oracle sequence with identity column using target table name
        key_col = info.get('key_column', 'EST_KEY')
        target_table = info.get('target_table', 'event_status')
        
        # Convert target table name to sequence name (e.g., 'event_status' -> 'EVENT_STATUS')
        sequence_table_name = target_table.upper()
        
        # Updated pattern to handle full catalog.schema.sequence format
        sequence_patterns = [
            # Pattern 1: Full catalog.schema.sequence format (e.g., gap_catalog.ads_owner.EVENT_STATUS_S.nextval)
            rf'[\w.]+\.{sequence_table_name}_S\.nextval(\s+as\s+{key_col})',
            # Pattern 2: Simple schema.sequence format (e.g., ADS_OWNER.EVENT_STATUS_S.nextval)
            rf'\w+\.{sequence_table_name}_S\.nextval(\s+as\s+{key_col})',
            # Pattern 3: Just sequence name (e.g., EVENT_STATUS_S.nextval)
            rf'{sequence_table_name}_S\.nextval(\s+as\s+{key_col})'
        ]
        
        for pattern in sequence_patterns:
            sql = re.sub(pattern, f'{key_col}_NEW\\1', sql, flags=re.IGNORECASE)
        
        # Replace process key numbers specifically
        sql = re.sub(r'\b\d{7,8}\b(?=\s+as\s+\w+_INSERT_PROCESS_KEY)', ':p_process_key', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\b\d{7,8}\b(?=\s+as\s+\w+_UPDATE_PROCESS_KEY)', ':p_process_key', sql, flags=re.IGNORECASE)
        
        # Convert load date patterns for EST_VALID_FROM
        sql = re.sub(r"to_date\('(\d{8})','YYYYMMDD'\)\s+as\s+EST_VALID_FROM", 
                     "to_date(':p_load_date','yyyy-MM-dd') as EST_VALID_FROM", sql, flags=re.IGNORECASE)
        
        return sql
    
    def generate_validation_section_notebook(self, info: Dict) -> str:
        """Generate validation queries - notebook version"""
        return f"""select * from (
select '1-Source_Table', count(1) rec_cnt from {self.schema_mappings['ADS_ETL_OWNER']}.DLK_ADS_LOV_RDS_ANALYTICALEVENTSTATUS where sys = 'Brasil'
union all
select '2-{info['xc_table']}', count(1) from {self.schema_mappings['ADS_ETL_OWNER']}.{info['xc_table']}
union all
select '3-DIFF_TABLE', count(1) from  ${{var.dif_table_name}}
union all
select '4-{info['target_table']}', count(1) from {self.schema_mappings['ADS_OWNER']}.{info['target_table']} where {info['key_column'].lower().replace('_key', '_source_sys_origin')} = 'RDS_ANALYTICALEVENTSTATUS'
) order by 1"""

def convert_file(input_file: str, output_file: str, output_format: str = 'sql', p_load_date: str = '2025-08-31'):
    """Convert a single Oracle SQL file to Databricks notebook or SQL script"""
    converter = OracleToDatabricsConverter()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        oracle_sql = f.read()
    
    if output_format.lower() == 'notebook':
        databricks_output = converter.generate_databricks_notebook(oracle_sql, p_load_date)
    else:  # default to SQL
        databricks_output = converter.generate_databricks_sql(oracle_sql, p_load_date)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(databricks_output)
    
    print(f"Converted {input_file} -> {output_file} ({output_format} format, load_date: {p_load_date})")

def batch_convert(input_directory: str, output_directory: str, output_format: str = 'sql', p_load_date: str = '2025-08-31'):
    """Convert all .sql files in a directory"""
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    for filename in os.listdir(input_directory):
        if filename.endswith('.sql'):
            input_path = os.path.join(input_directory, filename)
            
            if output_format.lower() == 'notebook':
                output_filename = filename.replace('.sql', '_DBX.py')
            else:
                output_filename = filename.replace('.sql', '_DBX.sql')
                
            output_path = os.path.join(output_directory, output_filename)
            
            try:
                convert_file(input_path, output_path, output_format, p_load_date)
            except Exception as e:
                print(f"Error converting {filename}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Convert single file to Notebook - FIXED VERSION
    # convert_file("ADS_RDS_EVENT_STATUS_ANALYTICAL_NEW.sql", "ADS_RDS_EVENT_STATUS_ANALYTICAL_FIXED.py", "notebook","2025-08-31")
    
    # Batch convert directory to Notebooks - FIXED VERSION  
    batch_convert("IMP_Map_Ora", "IMP_Map_DBX_FIXED", "notebook", "2025-08-31")
