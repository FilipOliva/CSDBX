# Databricks notebook source
# MAGIC %md
# MAGIC ## Copy DLK Tables from ads_owner to ads_etl_owner
# MAGIC - Purpose: Clean and copy all DLK tables between schemas
# MAGIC - Author: Data Engineer  
# MAGIC - Date: 2025-09-15
# MAGIC - Total Tables: 22

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration and Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries and Set Configuration
import time
from pyspark.sql import SparkSession
from datetime import datetime

# Set Spark configuration for optimal performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print(f"Process started at: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Define Table List with Expected Counts
# Define all DLK tables to be copied with their expected record counts
dlk_tables = [
    {"table_name": "dlk_ads_lov_rds_analyticaleventstatus", "expected_count": 526},
    {"table_name": "dlk_ads_lov_rds_analyticaleventtypes", "expected_count": 3836},
    {"table_name": "dlk_ads_lov_rds_conditionfulfillmentevent", "expected_count": 56},
    {"table_name": "dlk_ads_lov_rds_dispotransactiontype", "expected_count": 1372},
    {"table_name": "dlk_ads_lov_rds_distraintactstate", "expected_count": 84},
    {"table_name": "dlk_ads_lov_rds_distraintacttype", "expected_count": 319},
    {"table_name": "dlk_ads_lov_rds_distrainteventtype", "expected_count": 129},
    {"table_name": "dlk_ads_lov_rds_distraintstate", "expected_count": 154},
    {"table_name": "dlk_ads_lov_rds_errormessagebs", "expected_count": 5987},
    {"table_name": "dlk_ads_lov_rds_humantaskeventtype", "expected_count": 519},
    {"table_name": "dlk_ads_lov_rds_identificationaction", "expected_count": 279},
    {"table_name": "dlk_ads_lov_rds_identityregistrationaction", "expected_count": 444},
    {"table_name": "dlk_ads_lov_rds_identityregistrationcasestates", "expected_count": 114},
    {"table_name": "dlk_ads_lov_rds_mepoperationstate", "expected_count": 232},
    {"table_name": "dlk_ads_lov_rds_mepoperationtype", "expected_count": 512},
    {"table_name": "dlk_ads_lov_rds_personidentificationstate", "expected_count": 144},
    {"table_name": "dlk_ads_lov_rds_sendnotificationstatus", "expected_count": 39},
    {"table_name": "dlk_ads_rds_onboardingeligibilitycheckresult", "expected_count": 36},
    {"table_name": "dlk_ads_rds_onboardingidentificationresult", "expected_count": 26},
    {"table_name": "dlk_ads_rds_onboardingprocessresult", "expected_count": 16},
    {"table_name": "dlk_ads_rds_onboardingprocessstate", "expected_count": 86},
    {"table_name": "dlk_ads_rds_onboardingproductresult", "expected_count": 46}
]

# Schema definitions
source_schema = "gap_catalog.ads_owner"
target_schema = "gap_catalog.ads_etl_owner"

print(f"Total tables to process: {len(dlk_tables)}")
print(f"Source schema: {source_schema}")
print(f"Target schema: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy Tables Process

# COMMAND ----------

# DBTITLE 1,Copy Tables Function
def copy_dlk_table(table_name, expected_count):
    """
    Copy a single DLK table from source to target schema
    
    Args:
        table_name (str): Name of the table to copy
        expected_count (int): Expected number of records
        
    Returns:
        dict: Result with status and counts
    """
    
    source_table = f"{source_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"
    
    try:
        start_time = time.time()
        
        # Step 1: Truncate target table
        print(f"  Truncating {target_table}...")
        spark.sql(f"TRUNCATE TABLE {target_table}")
        
        # Step 2: Insert data from source
        print(f"  Copying data from {source_table}...")
        spark.sql(f"""
            INSERT INTO {target_table}
            SELECT * FROM {source_table}
        """)
        
        # Step 3: Get actual count
        actual_count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()
        actual_count = actual_count_result[0]['cnt']
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        status = "SUCCESS" if actual_count == expected_count else "COUNT_MISMATCH"
        
        result = {
            "table_name": table_name,
            "status": status,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "duration_seconds": duration
        }
        
        print(f"  ✓ Completed: {actual_count} records copied in {duration}s")
        
        if status == "COUNT_MISMATCH":
            print(f"  ⚠️  WARNING: Expected {expected_count}, got {actual_count}")
            
        return result
        
    except Exception as e:
        print(f"  ❌ ERROR: {str(e)}")
        return {
            "table_name": table_name,
            "status": "ERROR",
            "expected_count": expected_count,
            "actual_count": 0,
            "duration_seconds": 0,
            "error": str(e)
        }

# COMMAND ----------

# DBTITLE 1,Execute Copy Process
# Initialize results tracking
results = []
total_expected = sum([table["expected_count"] for table in dlk_tables])
total_actual = 0
successful_tables = 0
failed_tables = 0

print("=" * 80)
print("STARTING DLK TABLES COPY PROCESS")
print("=" * 80)

# Process each table
for i, table_config in enumerate(dlk_tables, 1):
    table_name = table_config["table_name"]
    expected_count = table_config["expected_count"]
    
    print(f"\n[{i}/{len(dlk_tables)}] Processing: {table_name}")
    
    result = copy_dlk_table(table_name, expected_count)
    results.append(result)
    
    if result["status"] == "SUCCESS":
        successful_tables += 1
        total_actual += result["actual_count"]
    elif result["status"] == "COUNT_MISMATCH":
        successful_tables += 1  # Still copied, just count mismatch
        total_actual += result["actual_count"]
    else:
        failed_tables += 1

print("\n" + "=" * 80)
print("COPY PROCESS COMPLETED")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results and Validation

# COMMAND ----------

# DBTITLE 1,Process Summary
# Display summary
print(f"PROCESS SUMMARY:")
print(f"- Total tables processed: {len(dlk_tables)}")
print(f"- Successful: {successful_tables}")
print(f"- Failed: {failed_tables}")
print(f"- Total expected records: {total_expected:,}")
print(f"- Total actual records: {total_actual:,}")
print(f"- Records match: {'✓ YES' if total_expected == total_actual else '❌ NO'}")

# COMMAND ----------

# DBTITLE 1,Detailed Results Table
# Convert results to DataFrame for better display
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("expected_count", IntegerType(), True),
    StructField("actual_count", IntegerType(), True),
    StructField("duration_seconds", DoubleType(), True)
])

# Prepare data for DataFrame (exclude error field for now)
df_data = []
for result in results:
    df_data.append((
        result["table_name"],
        result["status"],
        result["expected_count"],
        result["actual_count"],
        result["duration_seconds"]
    ))

results_df = spark.createDataFrame(df_data, schema)
results_df.createOrReplaceTempView("copy_results")

print("Detailed Results:")
results_df.show(25, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validation Query
# MAGIC %sql
# MAGIC -- Validation summary with status indicators
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     status,
# MAGIC     expected_count,
# MAGIC     actual_count,
# MAGIC     CASE 
# MAGIC         WHEN expected_count = actual_count THEN '✓'
# MAGIC         ELSE '❌'
# MAGIC     END as count_match,
# MAGIC     duration_seconds
# MAGIC FROM copy_results
# MAGIC ORDER BY 
# MAGIC     CASE WHEN status = 'ERROR' THEN 1
# MAGIC          WHEN status = 'COUNT_MISMATCH' THEN 2
# MAGIC          ELSE 3 END,
# MAGIC     table_name

# COMMAND ----------

# DBTITLE 1,Error Report
# Show any errors that occurred
error_results = [r for r in results if r["status"] == "ERROR"]

if error_results:
    print("❌ ERRORS ENCOUNTERED:")
    print("-" * 50)
    for error in error_results:
        print(f"Table: {error['table_name']}")
        print(f"Error: {error.get('error', 'Unknown error')}")
        print("-" * 50)
else:
    print("✓ No errors encountered during the copy process")

# COMMAND ----------

# DBTITLE 1,Performance Metrics
# Calculate performance metrics
total_duration = sum([r["duration_seconds"] for r in results])
avg_duration = total_duration / len(results) if results else 0
fastest_table = min(results, key=lambda x: x["duration_seconds"]) if results else None
slowest_table = max(results, key=lambda x: x["duration_seconds"]) if results else None

print("PERFORMANCE METRICS:")
print(f"- Total execution time: {total_duration:.2f} seconds")
print(f"- Average time per table: {avg_duration:.2f} seconds")
if fastest_table:
    print(f"- Fastest table: {fastest_table['table_name']} ({fastest_table['duration_seconds']:.2f}s)")
if slowest_table:
    print(f"- Slowest table: {slowest_table['table_name']} ({slowest_table['duration_seconds']:.2f}s)")

print(f"\nProcess completed at: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: Re-run Failed Tables
# MAGIC Run this cell if you need to retry any failed table copies

# COMMAND ----------

# DBTITLE 1,Retry Failed Tables (Optional)
# Get list of failed tables
failed_results = [r for r in results if r["status"] == "ERROR"]

if failed_results:
    print(f"Found {len(failed_results)} failed tables. Retrying...")
    
    retry_results = []
    for failed_result in failed_results:
        table_name = failed_result["table_name"]
        expected_count = failed_result["expected_count"]
        
        print(f"\nRetrying: {table_name}")
        retry_result = copy_dlk_table(table_name, expected_count)
        retry_results.append(retry_result)
    
    # Show retry results
    print("\nRETRY RESULTS:")
    for result in retry_results:
        status_icon = "✓" if result["status"] == "SUCCESS" else "❌"
        print(f"{status_icon} {result['table_name']}: {result['status']}")
else:
    print("No failed tables to retry.")
