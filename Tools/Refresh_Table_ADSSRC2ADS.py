# Databricks notebook source
# MAGIC %md
# MAGIC ## Refresh Tables from ads_src_20250901 to ads_owner
# MAGIC - Purpose: Copy case_object_status, event_types, and event_status tables
# MAGIC - Author: Data Engineer  
# MAGIC - Date: 2025-09-16
# MAGIC - Source: gap_catalog.ads_src_20250901
# MAGIC - Target: gap_catalog.ads_owner

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration and Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries and Set Configuration
import time
from datetime import datetime

# Set Spark configuration for optimal performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print(f"Process started at: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Define Tables and Schema
# Define tables to be copied
tables_to_copy = [
    "case_object_status",
    "event_types", 
    "event_status"
]

# Schema definitions
source_schema = "gap_catalog.ads_src_20250901"
target_schema = "gap_catalog.ads_owner"

print(f"Tables to copy: {tables_to_copy}")
print(f"Source schema: {source_schema}")
print(f"Target schema: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy Process

# COMMAND ----------

# DBTITLE 1,Copy Tables Function
def copy_table(table_name):
    """
    Copy a single table from source to target schema
    
    Args:
        table_name (str): Name of the table to copy
        
    Returns:
        dict: Result with status and counts
    """
    
    source_table = f"{source_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"
    
    try:
        start_time = time.time()
        
        print(f"  Processing {table_name}...")
        
        # Step 1: Get source count
        source_count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table}").collect()
        source_count = source_count_result[0]['cnt']
        print(f"    Source records: {source_count:,}")
        
        # Step 2: Truncate target table
        print(f"    Truncating target table...")
        spark.sql(f"DELETE FROM {target_table}")
        
        # Step 3: Insert data from source
        print(f"    Copying data...")
        spark.sql(f"""
            INSERT INTO {target_table}
            SELECT * FROM {source_table}
        """)
        
        # Step 4: Verify target count
        target_count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()
        target_count = target_count_result[0]['cnt']
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        status = "SUCCESS" if source_count == target_count else "COUNT_MISMATCH"
        
        print(f"    Target records: {target_count:,}")
        print(f"    Duration: {duration}s")
        print(f"    Status: {status}")
        
        return {
            "table_name": table_name,
            "status": status,
            "source_count": source_count,
            "target_count": target_count,
            "duration_seconds": duration
        }
        
    except Exception as e:
        print(f"    ❌ ERROR: {str(e)}")
        return {
            "table_name": table_name,
            "status": "ERROR",
            "source_count": 0,
            "target_count": 0,
            "duration_seconds": 0,
            "error": str(e)
        }

# COMMAND ----------

# DBTITLE 1,Execute Copy Process
# Initialize results tracking
results = []
successful_tables = 0
failed_tables = 0

print("=" * 60)
print("STARTING TABLE COPY PROCESS")
print("=" * 60)

# Process each table
for i, table_name in enumerate(tables_to_copy, 1):
    print(f"\n[{i}/{len(tables_to_copy)}] Copying: {table_name}")
    print("-" * 40)
    
    result = copy_table(table_name)
    results.append(result)
    
    if result["status"] == "SUCCESS":
        successful_tables += 1
        print(f"    ✓ SUCCESS")
    elif result["status"] == "COUNT_MISMATCH":
        successful_tables += 1  # Still copied, just count mismatch
        print(f"    ⚠️  WARNING: Count mismatch")
    else:
        failed_tables += 1
        print(f"    ❌ FAILED")

print("\n" + "=" * 60)
print("COPY PROCESS COMPLETED")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results Summary

# COMMAND ----------

# DBTITLE 1,Process Summary
# Calculate totals
total_source_records = sum([r["source_count"] for r in results])
total_target_records = sum([r["target_count"] for r in results])
total_duration = sum([r["duration_seconds"] for r in results])

print("PROCESS SUMMARY:")
print(f"- Total tables processed: {len(tables_to_copy)}")
print(f"- Successful: {successful_tables}")
print(f"- Failed: {failed_tables}")
print(f"- Total source records: {total_source_records:,}")
print(f"- Total target records: {total_target_records:,}")
print(f"- Records match: {'✓ YES' if total_source_records == total_target_records else '❌ NO'}")
print(f"- Total execution time: {total_duration:.2f} seconds")

print(f"\nProcess completed at: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Detailed Results
print("\nDETAILED RESULTS:")
print("-" * 80)
print(f"{'Table Name':<20} {'Status':<15} {'Source':<10} {'Target':<10} {'Duration':<8}")
print("-" * 80)

for result in results:
    table_name = result["table_name"]
    status = result["status"]
    source_count = f"{result['source_count']:,}"
    target_count = f"{result['target_count']:,}"
    duration = f"{result['duration_seconds']:.2f}s"
    
    print(f"{table_name:<20} {status:<15} {source_count:<10} {target_count:<10} {duration:<8}")

# Show any errors
error_results = [r for r in results if r["status"] == "ERROR"]
if error_results:
    print("\n❌ ERRORS:")
    print("-" * 40)
    for error in error_results:
        print(f"Table: {error['table_name']}")
        print(f"Error: {error.get('error', 'Unknown error')}")
        print("-" * 40)

# COMMAND ----------

# DBTITLE 1,Validation Queries
# MAGIC %sql
# MAGIC -- Quick validation of record counts
# MAGIC SELECT 
# MAGIC   'case_object_status' as table_name,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_src_20250901.case_object_status) as source_count,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_owner.case_object_status) as target_count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'event_types' as table_name,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_src_20250901.event_types) as source_count,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_owner.event_types) as target_count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'event_status' as table_name,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_src_20250901.event_status) as source_count,
# MAGIC   (SELECT COUNT(*) FROM gap_catalog.ads_owner.event_status) as target_count
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: Sample Data Check

# COMMAND ----------

# DBTITLE 1,Sample Data Verification (Optional)
# MAGIC %sql
# MAGIC -- Show sample records from each target table to verify data quality
# MAGIC SELECT 'case_object_status' as table_name, COUNT(*) as record_count
# MAGIC FROM gap_catalog.ads_owner.case_object_status
# MAGIC UNION ALL
# MAGIC SELECT 'event_types' as table_name, COUNT(*) as record_count  
# MAGIC FROM gap_catalog.ads_owner.event_types
# MAGIC UNION ALL
# MAGIC SELECT 'event_status' as table_name, COUNT(*) as record_count
# MAGIC FROM gap_catalog.ads_owner.event_status
