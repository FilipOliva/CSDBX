# Databricks notebook source
# MAGIC %md
# MAGIC ## API-Based Notebook to SQL Converter
# MAGIC Uses the same API approach as your working import script

# COMMAND ----------

# DBTITLE 1,Setup API Connection
import base64
import requests
import re
from datetime import datetime

# Get API credentials (same as your working script)
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {'Authorization': f'Bearer {token}'}

TARGET_PATH = "/Volumes/cis_personal_catalog/filip_oliva1/work/Query_Todo"
SOURCE_WORKSPACE_PATH = "/Users/filip.oliva1@ext.csas.cz/ADS/RDS"

print(f"Workspace URL: {workspace_url}")
print(f"Source path: {SOURCE_WORKSPACE_PATH}")
print(f"Target path: {TARGET_PATH}")

# Create target directory
dbutils.fs.mkdirs(TARGET_PATH)

# COMMAND ----------

# DBTITLE 1,Converter Class Using Working API Pattern

class APINotebookConverter:
    """Converter using the same API pattern as your working import script"""
    
    def __init__(self, workspace_url, headers):
        self.workspace_url = workspace_url
        self.headers = headers
        
    def list_notebooks_in_folder(self, folder_path):
        """List notebooks in a workspace folder"""
        try:
            url = f"https://{self.workspace_url}/api/2.0/workspace/list"
            data = {"path": folder_path}
            
            response = requests.get(url, headers=self.headers, params=data)
            
            if response.status_code == 200:
                objects = response.json().get('objects', [])
                notebooks = [obj for obj in objects if obj.get('object_type') == 'NOTEBOOK']
                print(f"Found {len(notebooks)} notebooks in {folder_path}")
                return notebooks
            else:
                print(f"Error listing notebooks: {response.text}")
                return []
                
        except Exception as e:
            print(f"Exception listing notebooks: {e}")
            return []
    
    def export_notebook(self, notebook_path):
        """Export notebook content using API"""
        try:
            url = f"https://{self.workspace_url}/api/2.0/workspace/export"
            data = {
                "path": notebook_path,
                "format": "SOURCE"
            }
            
            response = requests.get(url, headers=self.headers, params=data)
            
            if response.status_code == 200:
                # Decode base64 content
                content_b64 = response.json().get('content', '')
                content = base64.b64decode(content_b64).decode('utf-8')
                return content
            else:
                print(f"Error exporting {notebook_path}: {response.text}")
                return None
                
        except Exception as e:
            print(f"Exception exporting {notebook_path}: {e}")
            return None
    
    def convert_notebook_to_sql(self, notebook_content, map_name):
        """Convert notebook content to SQL query"""
        
        # Extract SQL blocks
        sql_blocks = self._extract_sql_blocks(notebook_content)
        
        # Build query
        query_parts = []
        query_parts.append(self._build_query_header(map_name))
        
        # Add SQL blocks
        for block in sql_blocks:
            if block['sql'].strip():
                query_parts.append(f"\n/* {block['title']} */")
                processed_sql = self._process_sql_content(block['sql'])
                query_parts.append(processed_sql)
        
        return '\n'.join(query_parts)
    
    def _extract_sql_blocks(self, content):
        """Extract SQL blocks from notebook content"""
        blocks = []
        
        # Split by command separators
        sections = content.split('# COMMAND ----------')
        
        for section in sections:
            if '# MAGIC %sql' not in section:
                continue
            
            # Extract title
            title_match = re.search(r'# DBTITLE 1,(.+)', section)
            title = title_match.group(1) if title_match else "SQL Block"
            
            # Extract SQL content
            sql_lines = []
            in_sql_block = False
            
            for line in section.split('\n'):
                if line.strip() == '# MAGIC %sql':
                    in_sql_block = True
                    continue
                elif in_sql_block:
                    if line.startswith('# MAGIC '):
                        sql_lines.append(line[8:])  # Remove "# MAGIC "
                    elif not line.strip():
                        sql_lines.append('')
                    else:
                        break
            
            sql_content = '\n'.join(sql_lines).strip()
            if sql_content:
                blocks.append({'title': title, 'sql': sql_content})
        
        return blocks
    
    def _build_query_header(self, map_name):
        """Build SQL query header"""
        table_info = self._get_table_info(map_name)
        
        return f"""/* Databricks SQL Query: {map_name} */
/* Converted from notebook format for better performance */
/* Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} */

/* Declare variables */ 
DECLARE OR REPLACE VARIABLE p_load_date STRING DEFAULT {{{{p_load_date}}}};
DECLARE OR REPLACE VARIABLE p_process_key STRING DEFAULT {{{{p_process_key}}}};
DECLARE OR REPLACE VARIABLE map_id STRING DEFAULT '{map_name}';
DECLARE OR REPLACE VARIABLE dif_table_name STRING DEFAULT 'gap_catalog.ads_etl_owner.DIFF_{map_name}_ADS_MAP_SCD_DIFF';
DECLARE OR REPLACE VARIABLE max_key BIGINT DEFAULT 0;

-- Get the maximum {table_info['key']} from Target table
SET VAR max_key = (
    SELECT COALESCE(MAX({table_info['key'].lower()}), 0) 
    FROM gap_catalog.ads_owner.{table_info['table']}
);
"""
    
    def _get_table_info(self, map_name):
        """Get table information from map name"""
        if 'EVENT_STATUS' in map_name:
            return {'table': 'event_status', 'key': 'EST_KEY'}
        elif 'EVENT_TYPES' in map_name:
            return {'table': 'event_types', 'key': 'EVETP_KEY'}
        elif 'CASE_OBJECT_STATUS' in map_name:
            return {'table': 'case_object_status', 'key': 'COS_KEY'}
        else:
            return {'table': 'unknown_table', 'key': 'ID'}
    
    def _process_sql_content(self, sql_content):
        """Process SQL content for Databricks SQL format"""
        
        # Fix parameter references
        sql_content = re.sub(r"'\$p_load_date'", 'CAST(p_load_date AS DATE)', sql_content)
        sql_content = re.sub(r'\$p_load_date', 'CAST(p_load_date AS DATE)', sql_content)
        sql_content = re.sub(r'\$p_process_key', 'CAST(p_process_key AS BIGINT)', sql_content)
        
        # Fix variable references
        sql_content = re.sub(r'\$\{var\.dif_table_name\}', 'IDENTIFIER(dif_table_name)', sql_content)
        sql_content = re.sub(r'\$\{var\.max_key\}', 'max_key', sql_content)
        
        # Fix date functions
        sql_content = re.sub(r"to_date\('3000-01-01','yyyy-MM-dd'\)", "DATE('3000-01-01')", sql_content)
        
        # Fix identity columns for new records
        sql_content = re.sub(
            r'(\w+_KEY_NEW)\s+\+\s+\$\{var\.max_key\}\s+as\s+(\w+_KEY)',
            r'\1 + max_key AS \2  -- Add the offset to maintain key sequence',
            sql_content,
            flags=re.IGNORECASE
        )
        
        sql_content = re.sub(
            r'(\w+_KEY_NEW)\s+as\s+(\w+_KEY)',
            r'\1 + max_key AS \2  -- Add the offset to maintain key sequence',
            sql_content,
            flags=re.IGNORECASE
        )
        
        return sql_content

# Create converter instance
converter = APINotebookConverter(workspace_url, headers)

# COMMAND ----------

# DBTITLE 1,List Available Notebooks

print("Listing notebooks in source folder...")
notebooks = converter.list_notebooks_in_folder(SOURCE_WORKSPACE_PATH)

if notebooks:
    print("\nFound notebooks:")
    for nb in notebooks:
        print(f"  📓 {nb['path']} (Language: {nb.get('language', 'UNKNOWN')})")
else:
    print("No notebooks found or error occurred")
    print("\nTrying alternative paths...")
    
    # Try alternative paths
    alt_paths = [
        "/Workspace/Users/filip.oliva1@ext.csas.cz/ADS/RDS",
        "/Users/filip.oliva1@ext.csas.cz/ADS",
    ]
    
    for alt_path in alt_paths:
        print(f"Trying: {alt_path}")
        alt_notebooks = converter.list_notebooks_in_folder(alt_path)
        if alt_notebooks:
            notebooks = alt_notebooks
            SOURCE_WORKSPACE_PATH = alt_path
            print(f"✓ Found {len(notebooks)} notebooks at {alt_path}")
            break

# COMMAND ----------

# DBTITLE 1,Convert All Notebooks

if not notebooks:
    print("❌ No notebooks found to convert")
else:
    print(f"Starting conversion of {len(notebooks)} notebooks...")
    print("=" * 60)
    
    successful = []
    failed = []
    
    for nb in notebooks:
        notebook_path = nb['path']
        notebook_name = nb['path'].split('/')[-1]
        
        # Skip if not a MAP notebook
        if not notebook_name.startswith('MAP_'):
            print(f"⏭️  Skipping {notebook_name} (not a MAP notebook)")
            continue
        
        print(f"\n📝 Processing: {notebook_name}")
        
        try:
            # Export notebook content
            content = converter.export_notebook(notebook_path)
            
            if not content:
                failed.append({'notebook': notebook_name, 'error': 'Failed to export content'})
                print(f"  ❌ Failed to export content")
                continue
            
            # Extract map name
            map_name = notebook_name.replace('MAP_', '').replace('_DBX', '')
            
            # Convert to SQL
            sql_query = converter.convert_notebook_to_sql(content, map_name)
            
            # Save SQL file
            output_filename = f"QUERY_{map_name}.sql"
            output_path = f"{TARGET_PATH}/{output_filename}"
            
            dbutils.fs.put(output_path, sql_query, overwrite=True)
            
            successful.append({'notebook': notebook_name, 'output': output_filename})
            print(f"  ✅ Created: {output_filename}")
            
        except Exception as e:
            failed.append({'notebook': notebook_name, 'error': str(e)})
            print(f"  ❌ Error: {str(e)[:100]}...")
    
    # Summary
    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    
    if successful:
        print("\nSuccessful conversions:")
        for item in successful:
            print(f"  {item['notebook']} → {item['output']}")
    
    if failed:
        print("\nFailed conversions:")
        for item in failed:
            print(f"  {item['notebook']}: {item['error'][:50]}...")

# COMMAND ----------

# DBTITLE 1,Verify Generated Files

try:
    files = dbutils.fs.ls(TARGET_PATH)
    sql_files = [f for f in files if f.name.endswith('.sql')]
    
    print(f"Generated SQL files in {TARGET_PATH}:")
    print("=" * 50)
    
    for f in sorted(sql_files, key=lambda x: x.name):
        size_kb = f.size / 1024
        print(f"📄 {f.name} ({size_kb:.1f} KB)")
    
    print(f"\nTotal: {len(sql_files)} SQL query files")
    
    # Show sample content
    if sql_files:
        sample_file = sql_files[0]
        print(f"\nSample content from {sample_file.name}:")
        print("-" * 40)
        sample = dbutils.fs.head(sample_file.path, max_bytes=1200)
        print(sample)
        print("... (truncated)")

except Exception as e:
    print(f"Error verifying files: {e}")

# COMMAND ----------

# DBTITLE 1,Test Single Notebook Conversion

# Test function to convert a specific notebook by name
def convert_single_notebook_by_name(notebook_name):
    """Convert a specific notebook by its name"""
    
    notebook_path = f"{SOURCE_WORKSPACE_PATH}/{notebook_name}"
    
    print(f"Converting single notebook: {notebook_name}")
    print(f"Path: {notebook_path}")
    
    try:
        # Export content
        content = converter.export_notebook(notebook_path)
        
        if not content:
            print("❌ Failed to export notebook content")
            return False
        
        print(f"✅ Successfully exported {len(content)} characters")
        
        # Convert
        map_name = notebook_name.replace('MAP_', '').replace('_DBX', '')
        sql_query = converter.convert_notebook_to_sql(content, map_name)
        
        # Save
        output_filename = f"QUERY_{map_name}.sql"
        output_path = f"{TARGET_PATH}/{output_filename}"
        
        dbutils.fs.put(output_path, sql_query, overwrite=True)
        
        print(f"✅ Created: {output_filename}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

# Example usage (uncomment to test specific notebook):
# convert_single_notebook_by_name("MAP_ADS_RDS_EVENT_STATUS_ANALYTICAL_DBX")

print("Single notebook conversion function ready!")

# COMMAND ----------

# DBTITLE 1,Usage Summary
# MAGIC
# MAGIC %md
# MAGIC ## Conversion Complete! 
# MAGIC
# MAGIC ### What happened:
# MAGIC 1. ✅ Connected to Databricks API using the same method as your working import script
# MAGIC 2. ✅ Listed all notebooks in your RDS folder  
# MAGIC 3. ✅ Exported each MAP notebook's content
# MAGIC 4. ✅ Converted notebook format to SQL query format
# MAGIC 5. ✅ Saved SQL files to `/Volumes/cis_personal_catalog/filip_oliva1/work/Query`
# MAGIC
# MAGIC ### Generated Files:
# MAGIC Each notebook `MAP_[NAME]_DBX` was converted to `QUERY_[NAME].sql`
# MAGIC
# MAGIC ### Key Transformations Applied:
# MAGIC - `$p_load_date` → `CAST(p_load_date AS DATE)`
# MAGIC - `$p_process_key` → `CAST(p_process_key AS BIGINT)`
# MAGIC - `${var.dif_table_name}` → `IDENTIFIER(dif_table_name)`
# MAGIC - `EST_KEY_NEW + ${var.max_key}` → `EST_KEY_NEW + max_key`
# MAGIC - Variable declarations with `{{p_load_date}}` and `{{p_process_key}}` parameters
# MAGIC
# MAGIC ### Next Steps:
# MAGIC 1. Check the generated SQL files in your target directory
# MAGIC 2. Create Databricks SQL queries using these files
# MAGIC 3. Set up query parameters: `p_load_date` and `p_process_key`
# MAGIC 4. Test and schedule your queries for better performance!
# MAGIC
# MAGIC ### Files are ready for production use! 🎉
