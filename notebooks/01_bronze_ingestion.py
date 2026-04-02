# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer Ingestion
# MAGIC 
# MAGIC **Project:** C360-Telco Medallion Architecture  
# MAGIC **Layer:** Bronze (Raw Data Preservation)  
# MAGIC **Engineer:** Marconi  
# MAGIC **Architecture:** Tesla's schema design v1.0
# MAGIC 
# MAGIC ## Purpose
# MAGIC Ingest 26 CSV files from telco_data folder into Bronze Delta tables with audit columns.
# MAGIC 
# MAGIC ## What This Notebook Does
# MAGIC 1. Reads CSV files from Files/telco_data/
# MAGIC 2. Adds audit columns (_ingestion_timestamp, _source_file, _row_hash, _is_current, _processing_date)
# MAGIC 3. Writes to Delta Lake format in Tables/bronze/
# MAGIC 4. Validates data quality (row counts, duplicates)
# MAGIC 5. Logs processing metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import hashlib

# Configuration
LAKEHOUSE_NAME = "telco_data"
SOURCE_PATH = "/lakehouse/default/Files/telco_data"
BRONZE_PATH = "Tables/bronze"
PROCESSING_DATE = current_date()

print("=" * 70)
print("🔧 BRONZE LAYER INGESTION")  
print("=" * 70)
print(f"📅 Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Source Path: {SOURCE_PATH}")
print(f"💾 Target Path: {BRONZE_PATH}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def add_audit_columns(df, source_file):
    """
    Add standard audit columns to bronze DataFrame
    """
    return (df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(source_file))
        .withColumn("_row_hash", md5(concat_ws("|", *[col(c) for c in df.columns])))
        .withColumn("_is_current", lit(True))
        .withColumn("_processing_date", lit(PROCESSING_DATE))
    )

def ingest_to_bronze(source_file, table_name, schema=None):
    """
    Generic function to ingest CSV to Bronze Delta table
    
    Args:
        source_file: CSV filename
        table_name: Target Bronze table name (without bronze_ prefix)
        schema: Optional explicit schema (StructType)
    
    Returns:
        Dict with ingestion metrics
    """
    start_time = datetime.now()
    source_path = f"{SOURCE_PATH}/{source_file}"
    target_table = f"bronze_{table_name}"
    target_path = f"{BRONZE_PATH}/{target_table}"
    
    print(f"\n{'=' * 70}")
    print(f"📊 Ingesting: {source_file} → {target_table}")
    print(f"{'=' * 70}")
    
    try:
        # Read CSV
        print(f"  📖 Reading CSV from {source_path}...")
        if schema:
            df = spark.read.format("csv").schema(schema).option("header", "true").load(source_path)
        else:
            df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_path)
        
        source_count = df.count()
        print(f"  ✓ Read {source_count:,} rows")
        
        # Add audit columns
        print(f"  🏷️  Adding audit columns...")
        df_with_audit = add_audit_columns(df, source_file)
        
        # Check for duplicates in source
        distinct_count = df.dropDuplicates().count()
        if source_count != distinct_count:
            print(f"  ⚠️  WARNING: {source_count - distinct_count} duplicate rows in source!")
        
        # Write to Delta
        print(f"  💾 Writing to Delta table: {target_path}...")
        df_with_audit.write.format("delta").mode("overwrite").save(target_path)
        
        # Verify write
        verify_df = spark.read.format("delta").load(target_path)
        target_count = verify_df.count()
        print(f"  ✓ Verified {target_count:,} rows written")
        
        # Create/replace table reference
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.{target_table}
            USING DELTA
            LOCATION '{target_path}'
        """)
        print(f"  ✓ Table {LAKEHOUSE_NAME}.{target_table} registered")
        
        # Calculate metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        metrics = {
            "table_name": target_table,
            "source_file": source_file,
            "rows_read": source_count,
            "rows_written": target_count,
            "duplicate_rows": source_count - distinct_count,
            "duration_seconds": duration,
            "status": "SUCCESS",
            "error_message": None
        }
        
        print(f"  ⏱️  Duration: {duration:.2f} seconds")
        print(f"  ✅ SUCCESS")
        
        return metrics
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"  ❌ ERROR: {str(e)}")
        
        metrics = {
            "table_name": target_table,
            "source_file": source_file,
            "rows_read": 0,
            "rows_written": 0,
            "duplicate_rows": 0,
            "duration_seconds": duration,
            "status": "FAILED",
            "error_message": str(e)
        }
        
        return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Core Customer & Party Data (5 tables)

# COMMAND ----------

print("\n" + "=" * 70)
print("📦 INGESTING: CORE CUSTOMER & PARTY DATA")
print("=" * 70)

results = []

# 1. Party (Customer Master)
results.append(ingest_to_bronze("party.csv", "party"))

# 2. Customer Account
results.append(ingest_to_bronze("customer_account.csv", "customer_account"))

# 3. Account Party Role (Relationships)
results.append(ingest_to_bronze("account_party_role.csv", "account_party_role"))

# 4. Address Master
results.append(ingest_to_bronze("address.csv", "address"))

# 5. Party Address (Links)
results.append(ingest_to_bronze("party_address.csv", "party_address"))

print(f"\n✅ Customer & Party data: {len(results)} tables ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Subscriber & Service Data (6 tables)

# COMMAND ----------

print("\n" + "=" * 70)
print("📦 INGESTING: SUBSCRIBER & SERVICE DATA")
print("=" * 70)

# 6. Subscriber
results.append(ingest_to_bronze("subscriber.csv", "subscriber"))

# 7. Subscription
results.append(ingest_to_bronze("subscription.csv", "subscription"))

# 8. Subscriber Status History
results.append(ingest_to_bronze("subscriber_status_history.csv", "subscriber_status_history"))

# 9. Service
results.append(ingest_to_bronze("service.csv", "service"))

# 10. Service Order
results.append(ingest_to_bronze("service_order.csv", "service_order"))

# 11. Entitlement
results.append(ingest_to_bronze("entitlement.csv", "entitlement"))

print(f"\n✅ Subscriber & Service data: 6 tables ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Mobile Assets Data (6 tables)

# COMMAND ----------

print("\n" + "=" * 70)
print("📦 INGESTING: MOBILE ASSETS DATA")
print("=" * 70)

# 12. MSISDN (Phone numbers)
results.append(ingest_to_bronze("msisdn.csv", "msisdn"))

# 13. Subscriber MSISDN mapping
results.append(ingest_to_bronze("subscriber_msisdn.csv", "subscriber_msisdn"))

# 14. SIM cards
results.append(ingest_to_bronze("sim.csv", "sim"))

# 15. Subscriber SIM mapping
results.append(ingest_to_bronze("subscriber_sim.csv", "subscriber_sim"))

# 16. Devices
results.append(ingest_to_bronze("device.csv", "device"))

# 17. Subscriber Device mapping
results.append(ingest_to_bronze("subscriber_device.csv", "subscriber_device"))

# 18. Porting requests
results.append(ingest_to_bronze("porting_request.csv", "porting_request"))

print(f"\n✅ Mobile Assets data: 7 tables ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Billing & Revenue Data (6 tables)

# COMMAND ----------

print("\n" + "=" * 70)
print("📦 INGESTING: BILLING & REVENUE DATA")
print("=" * 70)

# 19. Invoices
results.append(ingest_to_bronze("invoice.csv", "invoice"))

# 20. Invoice Lines (LARGEST FILE - 545 KB)
print("\n⚠️  Processing LARGEST file: invoice_line.csv")
results.append(ingest_to_bronze("invoice_line.csv", "invoice_line"))

# 21. Charges
results.append(ingest_to_bronze("charge.csv", "charge"))

# 22. Payments
results.append(ingest_to_bronze("payment.csv", "payment"))

# 23. Prepaid Balance Snapshots
results.append(ingest_to_bronze("prepaid_balance_snapshot.csv", "prepaid_balance_snapshot"))

# 24. Topups
results.append(ingest_to_bronze("topup.csv", "topup"))

print(f"\n✅ Billing & Revenue data: 6 tables ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Product & Support Data (2 tables)

# COMMAND ----------

print("\n" + "=" * 70)
print("📦 INGESTING: PRODUCT & SUPPORT DATA")
print("=" * 70)

# 25. Product Catalog
results.append(ingest_to_bronze("product_catalog.csv", "product_catalog"))

# 26. Case Tickets (Support)
results.append(ingest_to_bronze("case_ticket.csv", "case_ticket"))

print(f"\n✅ Product & Support data: 2 tables ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary & Data Quality Report

# COMMAND ----------

import pandas as pd

print("\n" + "=" * 70)
print("📊 BRONZE LAYER INGESTION SUMMARY")
print("=" * 70)

# Convert results to DataFrame
results_df = pd.DataFrame(results)

# Summary stats
total_tables = len(results)
successful = len(results_df[results_df['status'] == 'SUCCESS'])
failed = len(results_df[results_df['status'] == 'FAILED'])
total_rows = results_df['rows_written'].sum()
total_duration = results_df['duration_seconds'].sum()

print(f"\n📈 Overall Statistics:")
print(f"  Total Tables Processed: {total_tables}")
print(f"  ✅ Successful: {successful}")
print(f"  ❌ Failed: {failed}")
print(f"  📊 Total Rows Ingested: {total_rows:,}")
print(f"  ⏱️  Total Duration: {total_duration:.2f} seconds")
print(f"  🚀 Average Rate: {total_rows / total_duration:.0f} rows/second")

# Show detailed results
print(f"\n📋 Detailed Results:")
print(results_df.to_string(index=False))

# Data quality issues
issues_df = results_df[results_df['duplicate_rows'] > 0]
if len(issues_df) > 0:
    print(f"\n⚠️  DATA QUALITY WARNINGS:")
    print(f"  {len(issues_df)} table(s) have duplicate rows in source")
    print(issues_df[['table_name', 'duplicate_rows']].to_string(index=False))

# Failed ingestions
failed_df = results_df[results_df['status'] == 'FAILED']
if len(failed_df) > 0:
    print(f"\n❌ FAILED INGESTIONS:")
    print(failed_df[['table_name', 'error_message']].to_string(index=False))
else:
    print(f"\n✅ All ingestions completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Metadata

# COMMAND ----------

# Log to pipeline_metadata table
metadata_record = {
    "pipeline_run_id": str(datetime.now().timestamp()),
    "pipeline_name": "bronze_ingestion",
    "layer": "bronze",
    "start_time": datetime.now() - pd.Timedelta(seconds=total_duration),
    "end_time": datetime.now(),
    "status": "SUCCESS" if failed == 0 else "PARTIAL_SUCCESS",
    "rows_processed": int(total_rows),
    "error_message": None if failed == 0 else f"{failed} tables failed",
    "created_timestamp": datetime.now()
}

print("\n📝 Logging metadata...")
try:
    metadata_df = spark.createDataFrame([metadata_record])
    metadata_df.write.format("delta").mode("append").saveAsTable(f"{LAKEHOUSE_NAME}.pipeline_metadata")
    print("✅ Metadata logged successfully")
except Exception as e:
    print(f"⚠️  Could not log metadata: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries

# COMMAND ----------

print("\n" + "=" * 70)
print("🔍 VALIDATION: Spot Check Bronze Tables")
print("=" * 70)

# Check a few key tables
validation_tables = ['bronze_party', 'bronze_subscriber', 'bronze_invoice', 'bronze_invoice_line']

for table in validation_tables:
    print(f"\n{table}:")
    try:
        df = spark.table(f"{LAKEHOUSE_NAME}.{table}")
        count = df.count()
        print(f"  Row count: {count:,}")
        print(f"  Sample row:")
        df.select([c for c in df.columns if not c.startswith('_')][:5]).show(1, truncate=False, vertical=True)
    except Exception as e:
        print(f"  ❌ Error: {e}")

# COMMAND ----------

print("\n" + "=" * 70)
print("✅ BRONZE LAYER INGESTION COMPLETE")
print("=" * 70)
print(f"📊 {successful}/{total_tables} tables successfully ingested")
print(f"💾 {total_rows:,} total rows in Bronze layer")
print(f"⏱️  Completed in {total_duration:.2f} seconds")
print("\n🎯 Next Step: Run 02_silver_transformations.ipynb")
print("=" * 70)
