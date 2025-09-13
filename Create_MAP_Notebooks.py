import base64
import requests
import os

def batch_import_notebooks(source_dir, target_folder, overwrite=True, tag_value="ETL_Mapping"):
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {'Authorization': f'Bearer {token}'}
    
    # Get all .py files
    py_files = [f for f in os.listdir(source_dir) if f.endswith('.py')]
    print(f"Found {len(py_files)} Python files to import")
    
    success_count = 0
    failed_count = 0
    
    for filename in py_files:
        try:
            # Read file
            file_path = os.path.join(source_dir, filename)
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Create notebook name with MAP prefix
            notebook_name = "MAP_" + filename.replace('.py', '')
            notebook_path = f"{target_folder}/{notebook_name}"
            
            # Import notebook with overwrite option
            data = {
                "path": notebook_path,
                "content": base64.b64encode(content.encode()).decode(),
                "language": "PYTHON",
                "format": "SOURCE",
                "overwrite": overwrite  # This handles the RESOURCE_ALREADY_EXISTS error
            }
            
            response = requests.post(f"https://{workspace_url}/api/2.0/workspace/import", 
                                   headers=headers, json=data)
            
            if response.status_code == 200:
                print(f"✅ {filename} → {notebook_name}")
                success_count += 1
            else:
                print(f"❌ {filename} - Error: {response.text}")
                failed_count += 1
                
        except Exception as e:
            print(f"❌ {filename} - Exception: {e}")
            failed_count += 1
    
    print(f"\n📊 Results: ✅{success_count} ❌{failed_count}")

# Usage with overwrite enabled:
batch_import_notebooks(
    source_dir="/Volumes/cis_personal_catalog/filip_oliva1/work/Export_Map_Notebooks",
    target_folder="/Users/filip.oliva1@ext.csas.cz/ADS",
    overwrite=True
)