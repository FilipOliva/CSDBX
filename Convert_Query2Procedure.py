import json
import os
import re
from pathlib import Path

class QueryToProcedureConverter:
    def __init__(self, source_dir, target_dir):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.target_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_sql_from_notebook(self, notebook_path):
        """Extract SQL code from Databricks notebook JSON"""
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
            
            sql_code = ""
            for cell in notebook.get('cells', []):
                if cell.get('cell_type') == 'code':
                    source = cell.get('source', [])
                    if isinstance(source, list):
                        sql_code += ''.join(source)
                    else:
                        sql_code += source
            
            return sql_code.strip()
        except Exception as e:
            print(f"Error reading notebook {notebook_path}: {e}")
            return None
    
    def extract_parameters(self, sql_code):
        """Extract parameters from DECLARE statements"""
        parameters = []
        
        # Look for parameter declarations like {{p_load_date}} or {{p_process_key}}
        param_pattern = r'\{\{(\w+)\}\}'
        matches = re.findall(param_pattern, sql_code)
        
        for match in matches:
            if match not in [p['name'] for p in parameters]:
                parameters.append({
                    'name': match,
                    'type': 'STRING'  # Default to STRING, can be refined
                })
        
        return parameters
    
    def extract_table_info(self, sql_code):
        """Extract key information for procedure naming and logging"""
        # Extract source system origin
        origin_match = re.search(r"EVETP_SOURCE_SYS_ORIGIN\s*=\s*'([^']+)'", sql_code)
        if not origin_match:
            origin_match = re.search(r"'([^']*OPERATIONTYPE[^']*)'", sql_code)
        source_sys_origin = origin_match.group(1) if origin_match else 'UNKNOWN'
        
        # Extract target table - look for INSERT INTO pattern
        target_table_match = re.search(r'INSERT\s+INTO\s+gap_catalog\.ads_owner\.(\w+)', sql_code, re.IGNORECASE)
        target_table = target_table_match.group(1) if target_table_match else 'EVENT_TYPES'
        
        return source_sys_origin, target_table
    
    def extract_diff_table_name(self, sql_code):
        """Extract the diff table name from DECLARE statement"""
        # Look for DECLARE OR REPLACE VARIABLE dif_table_name STRING DEFAULT 'table_name';
        match = re.search(r"DECLARE OR REPLACE VARIABLE dif_table_name STRING DEFAULT '([^']+)';", sql_code)
        if match:
            return match.group(1)
        return None
    
    def clean_sql_code(self, sql_code):
        """Minimal cleaning of SQL code - only essential changes"""
        # Extract diff table name before cleaning
        diff_table_name = self.extract_diff_table_name(sql_code)
        
        # Remove Databricks-specific comments
        lines = sql_code.split('\n')
        cleaned_lines = []
        
        skip_line = False
        for line in lines:
            # Skip Databricks conversion comments
            if any(phrase in line.lower() for phrase in [
                'databricks sql query:', 'converted from notebook', 'generated:'
            ]):
                skip_line = True
                continue
            
            # Skip variable declarations - they become procedure parameters
            if line.strip().startswith('DECLARE OR REPLACE VARIABLE'):
                skip_line = True
                continue
            
            if skip_line and line.strip() == '':
                continue
            
            skip_line = False
            cleaned_lines.append(line)
        
        # Join lines back
        cleaned_sql = '\n'.join(cleaned_lines)
        
        # Only make essential replacements
        # Replace IDENTIFIER() calls with the actual diff table name from this query
        if diff_table_name:
            cleaned_sql = re.sub(
                r'IDENTIFIER\s*\(\s*dif_table_name\s*\)', 
                diff_table_name, 
                cleaned_sql,
                flags=re.IGNORECASE
            )
        
        # Fix the most common date function issues
        cleaned_sql = re.sub(r"to_date\('01013000','ddMMyyyy'\)", "DATE('3000-01-01')", cleaned_sql)
        cleaned_sql = re.sub(r"to_date\('30000101','yyyyMMdd'\)", "DATE('3000-01-01')", cleaned_sql)
        
        # Fix date arithmetic for load_date
        cleaned_sql = re.sub(
            r"to_date\(CAST\(p_load_date AS DATE\),'yyyy-MM-dd'\)\s*-\s*1", 
            "DATEADD(DAY, -1, CAST(p_load_date AS DATE))", 
            cleaned_sql
        )
        cleaned_sql = re.sub(
            r"to_date\(CAST\(p_load_date AS DATE\),'yyyy-MM-dd'\)", 
            "CAST(p_load_date AS DATE)", 
            cleaned_sql
        )
        
        return cleaned_sql.strip()
    
    def generate_procedure_name(self, query_filename):
        """Generate procedure name from query filename"""
        # Remove Q_ prefix and .dbquery.ipynb suffix
        base_name = query_filename.replace('Q_', '').replace('.dbquery.ipynb', '')
        return f"sp_{base_name}"
    
    def convert_to_procedure(self, sql_code, procedure_name, parameters, source_sys_origin, target_table):
        """Convert SQL code to stored procedure format with updated logging functions"""
        
        # Create parameter list for procedure signature
        param_list = []
        for param in parameters:
            param_list.append(f"    {param['name']} {param['type']}")
        
        param_signature = ",\n".join(param_list)
        
        # Clean the SQL code minimally
        main_logic = self.clean_sql_code(sql_code)
        
        # Properly indent the SQL code
        indented_lines = []
        for line in main_logic.split('\n'):
            if line.strip():  # Only indent non-empty lines
                indented_lines.append('        ' + line)
            else:
                indented_lines.append('')  # Keep empty lines as-is
        
        indented_logic = '\n'.join(indented_lines)
        
        # Generate the complete procedure - use the actual base name without sp_ for diff table
        base_name_for_diff = procedure_name[3:] if procedure_name.startswith('sp_') else procedure_name
        diff_table_name = f"gap_catalog.ads_etl_owner.DIFF_{base_name_for_diff}_ADS_MAP_SCD_DIFF"
        
        procedure_code = f"""-- Create SQL Procedure for {base_name_for_diff.upper()} ETL Process
CREATE OR REPLACE PROCEDURE gap_catalog.ads_etl_owner.{procedure_name}(
{param_signature}
)
LANGUAGE SQL
 SQL SECURITY INVOKER
COMMENT 'ETL procedure for {base_name_for_diff} - converted from notebook format'
AS
BEGIN
    -- Declare local variables
    DECLARE dif_table_name STRING DEFAULT '{diff_table_name}';
    DECLARE my_log_id STRING;
    BEGIN
        CALL gap_catalog.log.start_performance_test(
            test_case_name => 'ADS_RDS_PROC',
            step_name => '{base_name_for_diff}',
            psource_sys_origin  => '{source_sys_origin}',
            process_key => p_process_key,
            target_table => '{target_table}',
            log_id => my_log_id,
            p_load_date => p_load_date
        );

{indented_logic}

        CALL gap_catalog.log.complete_performance_test(
            plog_id => my_log_id
        );

    END;
    
END;"""

        return procedure_code
    
    def convert_single_file(self, source_file):
        """Convert a single query file to procedure format"""
        print(f"Converting {source_file.name}...")
        
        # Extract SQL from notebook
        sql_code = self.extract_sql_from_notebook(source_file)
        if not sql_code:
            print(f"Failed to extract SQL from {source_file.name}")
            return False
        
        # Extract parameters
        parameters = self.extract_parameters(sql_code)
        if not parameters:
            # Default parameters if none found
            parameters = [
                {'name': 'p_load_date', 'type': 'STRING'},
                {'name': 'p_process_key', 'type': 'STRING'}
            ]
        
        # Extract metadata
        source_sys_origin, target_table = self.extract_table_info(sql_code)
        
        # Generate procedure name
        procedure_name = self.generate_procedure_name(source_file.name)
        
        # Convert to procedure
        procedure_code = self.convert_to_procedure(
            sql_code, procedure_name, parameters, source_sys_origin, target_table
        )
        
        # Write output file
        output_filename = f"{procedure_name}.dbquery.ipynb"
        output_path = self.target_dir / output_filename
        
        # Create notebook structure for output
        output_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 0,
                    "metadata": {
                        "application/vnd.databricks.v1+cell": {
                            "cellMetadata": {},
                            "inputWidgets": {},
                            "nuid": "generated-procedure-id",
                            "showTitle": False,
                            "tableResultSettingsMap": {},
                            "title": ""
                        }
                    },
                    "outputs": [],
                    "source": [procedure_code]
                }
            ],
            "metadata": {
                "application/vnd.databricks.v1+notebook": {
                    "computePreferences": None,
                    "dashboards": [],
                    "environmentMetadata": None,
                    "inputWidgetPreferences": None,
                    "language": "sql",
                    "notebookMetadata": {
                        "sqlQueryOptions": {
                            "applyAutoLimit": True,
                            "catalog": "cis_catalog",
                            "schema": "default"
                        }
                    },
                    "notebookName": output_filename,
                    "widgets": {}
                },
                "language_info": {
                    "name": "sql"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 0
        }
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_notebook, f, indent=1)
            print(f"Successfully created {output_filename}")
            return True
        except Exception as e:
            print(f"Error writing {output_filename}: {e}")
            return False
    
    def convert_all_queries(self):
        """Convert all Q*.dbquery.ipynb files in source directory"""
        query_files = list(self.source_dir.glob("Q*.dbquery.ipynb"))
        
        if not query_files:
            print(f"No Q*.dbquery.ipynb files found in {self.source_dir}")
            return
        
        print(f"Found {len(query_files)} query files to convert")
        
        successful = 0
        failed = 0
        
        for query_file in query_files:
            try:
                if self.convert_single_file(query_file):
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"Unexpected error converting {query_file.name}: {e}")
                failed += 1
        
        print(f"\nConversion complete:")
        print(f"  Successfully converted: {successful}")
        print(f"  Failed: {failed}")

def main():
    # Configuration
    source_directory = "/Workspace/Users/filip.oliva1@ext.csas.cz/ADS/RDS/SQL_Query"
    target_directory = "/Workspace/Users/filip.oliva1@ext.csas.cz/ADS/RDS/Proc_DDL"
    
    print("Starting batch conversion of queries to procedures...")
    print(f"Source directory: {source_directory}")
    print(f"Target directory: {target_directory}")
    
    # Create converter and run conversion
    converter = QueryToProcedureConverter(source_directory, target_directory)
    converter.convert_all_queries()
    
    print("Batch conversion completed!")

if __name__ == "__main__":
    main()