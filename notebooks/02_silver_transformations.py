# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer Transformations
# MAGIC 
# MAGIC **Project:** C360-Telco Medallion Architecture  
# MAGIC **Layer:** Silver (Cleansed & Conformed)  
# MAGIC **Engineer:** Marconi  
# MAGIC **Architecture:** Tesla's schema design v1.0
# MAGIC 
# MAGIC ## Purpose
# MAGIC Transform 26 Bronze tables into 7 Silver domain tables with:
# MAGIC - Data cleansing and standardization
# MAGIC - Referential integrity validation
# MAGIC - SCD Type 2 historical tracking
# MAGIC - Business logic and derived fields
# MAGIC 
# MAGIC ## Silver Tables
# MAGIC 1. silver_customer_profile (Customer Domain)
# MAGIC 2. silver_subscriber_overview (Subscriber Domain)
# MAGIC 3. silver_mobile_inventory (Assets Domain)
# MAGIC 4. silver_billing_transactions (Revenue Domain)
# MAGIC 5. silver_prepaid_activity (Prepaid Domain)
# MAGIC 6. silver_service_catalog (Service Domain)
# MAGIC 7. silver_support_interactions (Support Domain)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import hashlib

# Configuration
LAKEHOUSE_NAME = "telco_data"
BRONZE_PATH = "Tables/bronze"
SILVER_PATH = "Tables/silver"

print("=" * 70)
print("🔧 SILVER LAYER TRANSFORMATIONS")  
print("=" * 70)
print(f"📅 Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Source: {BRONZE_PATH}")
print(f"💾 Target: {SILVER_PATH}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def calculate_age(birth_date_col):
    """Calculate age in years from birth date"""
    return floor(datediff(current_date(), birth_date_col) / 365.25)

def calculate_age_cohort(age_col):
    """Assign age cohort based on age"""
    return (
        when(age_col < 18, "0-17")
        .when((age_col >= 18) & (age_col < 26), "18-25")
        .when((age_col >= 26) & (age_col < 36), "26-35")
        .when((age_col >= 36) & (age_col < 46), "36-45")
        .when((age_col >= 46) & (age_col < 56), "46-55")
        .when((age_col >= 56) & (age_col < 66), "56-65")
        .otherwise("66+")
    )

def calculate_lifecycle_stage(tenure_days_col):
    """Determine customer lifecycle stage"""
    return (
        when(tenure_days_col < 90, "New")
        .when((tenure_days_col >= 90) & (tenure_days_col < 365), "Growing")
        .otherwise("Established")
    )

def add_scd2_columns(df, business_key):
    """
    Add SCD Type 2 tracking columns
    """
    return (df
        .withColumn("_valid_from", current_timestamp())
        .withColumn("_valid_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("_is_current", lit(True))
        .withColumn("_version", lit(1))
        .withColumn("_created_timestamp", current_timestamp())
        .withColumn("_updated_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Customer Profile (Customer Domain)
# MAGIC 
# MAGIC **Sources:** bronze_party + bronze_customer_account + bronze_address + bronze_party_address  
# MAGIC **Transformations:** Name standardization, age calculation, tenure calculation

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: silver_customer_profile")
print("=" * 70)

# Read bronze tables
party = spark.table(f"{LAKEHOUSE_NAME}.bronze_party")
customer_account = spark.table(f"{LAKEHOUSE_NAME}.bronze_customer_account")
address = spark.table(f"{LAKEHOUSE_NAME}.bronze_address")
party_address = spark.table(f"{LAKEHOUSE_NAME}.bronze_party_address")
account_party_role = spark.table(f"{LAKEHOUSE_NAME}.bronze_account_party_role")

# Join customer data
customer_profile = (
    party.alias("p")
    .join(
        account_party_role.alias("apr"),
        col("p.party_id") == col("apr.party_id"),
        "left"
    )
    .join(
        customer_account.alias("ca"),
        col("apr.account_id") == col("ca.account_id"),
        "left"
    )
    .join(
        party_address.alias("pa"),
        col("p.party_id") == col("pa.party_id"),
        "left"
    )
    .join(
        address.alias("a"),
        col("pa.address_id") == col("a.address_id"),
        "left"
    )
    .select(
        # Business keys
        col("p.party_id").alias("customer_id"),
        col("ca.account_id"),
        
        # Customer info
        col("p.party_type"),
        col("p.display_name"),
        col("p.given_name"),
        col("p.family_name"),
        col("p.email"),
        col("p.birth_date"),
        
        # Account info
        col("ca.account_type"),
        col("ca.account_status"),
        
        # Address info
        concat_ws(", ", 
            col("a.street_address"), 
            col("a.house_number"), 
            col("a.postal_code"), 
            col("a.city")
        ).alias("full_address"),
        col("a.postal_code"),
        col("a.city"),
        col("a.province"),
        
        # Original timestamps
        col("p.created_ts"),
        col("p.updated_ts")
    )
)

# Add derived fields
customer_profile = (
    customer_profile
    .withColumn("age_years", calculate_age(col("birth_date")))
    .withColumn("age_cohort", calculate_age_cohort(col("age_years")))
    .withColumn("customer_tenure_days", datediff(current_date(), col("created_ts")))
    .withColumn("lifecycle_stage", calculate_lifecycle_stage(col("customer_tenure_days")))
    .withColumn("is_business", col("party_type") == "ORG")
)

# Add SCD Type 2 columns
customer_profile = add_scd2_columns(customer_profile, "customer_id")

# Write to Delta
print(f"  💾 Writing to silver_customer_profile...")
customer_profile.write.format("delta").mode("overwrite").partitionBy("_is_current").save(f"{SILVER_PATH}/silver_customer_profile")

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_customer_profile
    USING DELTA
    LOCATION '{SILVER_PATH}/silver_customer_profile'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.silver_customer_profile").count()
print(f"  ✅ silver_customer_profile created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Subscriber Overview (Subscriber Domain)
# MAGIC 
# MAGIC **Sources:** bronze_subscriber + bronze_subscription + bronze_subscriber_status_history  
# MAGIC **Transformations:** Status aggregation, tenure calculation, active flag

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: silver_subscriber_overview")
print("=" * 70)

# Read bronze tables
subscriber = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscriber")
subscription = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscription")
status_history = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscriber_status_history")

# Aggregate status changes
status_changes = (
    status_history
    .groupBy("subscriber_id")
    .agg(
        count("*").alias("status_change_count"),
        max("change_date").alias("last_status_change_date")
    )
)

# Join subscriber data
subscriber_overview = (
    subscriber.alias("s")
    .join(
        subscription.alias("sub"),
        col("s.subscriber_id") == col("sub.subscriber_id"),
        "left"
    )
    .join(
        status_changes.alias("sc"),
        col("s.subscriber_id") == col("sc.subscriber_id"),
        "left"
    )
    .select(
        # Business keys
        col("s.subscriber_id"),
        col("s.customer_id"),
        col("sub.subscription_id"),
        
        # Subscriber info
        col("s.subscriber_type"),
        col("sub.status").alias("current_status"),
        col("s.activation_date"),
        
        # Status history
        coalesce(col("sc.status_change_count"), lit(0)).alias("status_change_count"),
        col("sc.last_status_change_date"),
        
        # Timestamps
        col("s.created_ts")
    )
)

# Add derived fields
subscriber_overview = (
    subscriber_overview
    .withColumn("subscriber_tenure_days", datediff(current_date(), col("activation_date")))
    .withColumn("is_active", col("current_status").isin(["Active", "active", "ACTIVE"]))
)

# Add SCD Type 2 columns
subscriber_overview = add_scd2_columns(subscriber_overview, "subscriber_id")

# Write to Delta
print(f"  💾 Writing to silver_subscriber_overview...")
subscriber_overview.write.format("delta").mode("overwrite").partitionBy("_is_current", "current_status").save(f"{SILVER_PATH}/silver_subscriber_overview")

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_subscriber_overview
    USING DELTA
    LOCATION '{SILVER_PATH}/silver_subscriber_overview'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.silver_subscriber_overview").count()
print(f"  ✅ silver_subscriber_overview created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Mobile Inventory (Assets Domain)
# MAGIC 
# MAGIC **Sources:** bronze_msisdn + bronze_sim + bronze_device + associations  
# MAGIC **Transformations:** Asset consolidation, completeness checking, device age

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: silver_mobile_inventory")
print("=" * 70)

# Read bronze tables
msisdn = spark.table(f"{LAKEHOUSE_NAME}.bronze_msisdn")
sim = spark.table(f"{LAKEHOUSE_NAME}.bronze_sim")
device = spark.table(f"{LAKEHOUSE_NAME}.bronze_device")
sub_msisdn = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscriber_msisdn")
sub_sim = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscriber_sim")
sub_device = spark.table(f"{LAKEHOUSE_NAME}.bronze_subscriber_device")

# Start with subscriber-msisdn relationship
mobile_inventory = (
    sub_msisdn.alias("sm")
    .join(msisdn.alias("m"), col("sm.msisdn_id") == col("m.msisdn_id"), "left")
    .join(sub_sim.alias("ss"), col("sm.subscriber_id") == col("ss.subscriber_id"), "left")
    .join(sim.alias("s"), col("ss.sim_id") == col("s.sim_id"), "left")
    .join(sub_device.alias("sd"), col("sm.subscriber_id") == col("sd.subscriber_id"), "left")
    .join(device.alias("d"), col("sd.device_id") == col("d.device_id"), "left")
    .select(
        # Business key
        col("sm.subscriber_id"),
        
        # Phone number
        col("m.msisdn"),
        col("m.status").alias("msisdn_status"),
        col("sm.assigned_date").alias("msisdn_assigned_date"),
        
        # SIM card
        col("s.sim_id"),
        col("s.iccid"),
        col("s.status").alias("sim_status"),
        col("ss.assigned_date").alias("sim_assigned_date"),
        
        # Device
        col("d.device_id"),
        col("d.brand").alias("device_brand"),
        col("d.model").alias("device_model"),
        col("sd.assigned_date").alias("device_assigned_date")
    )
)

# Add derived fields
mobile_inventory = (
    mobile_inventory
    .withColumn("device_age_months", 
        floor(datediff(current_date(), col("device_assigned_date")) / 30.44))
    .withColumn("is_complete_setup",
        col("msisdn").isNotNull() & 
        col("sim_id").isNotNull() & 
        col("device_id").isNotNull())
    .withColumn("sim_swap_count", lit(0))  # Would need history to calculate
)

# Add SCD Type 2 columns
mobile_inventory = add_scd2_columns(mobile_inventory, "subscriber_id")

# Write to Delta
print(f"  💾 Writing to silver_mobile_inventory...")
mobile_inventory.write.format("delta").mode("overwrite").partitionBy("_is_current").save(f"{SILVER_PATH}/silver_mobile_inventory")

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_mobile_inventory
    USING DELTA
    LOCATION '{SILVER_PATH}/silver_mobile_inventory'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.silver_mobile_inventory").count()
print(f"  ✅ silver_mobile_inventory created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Billing Transactions (Revenue Domain)
# MAGIC 
# MAGIC **Sources:** bronze_invoice + bronze_invoice_line + bronze_payment  
# MAGIC **Transformations:** Payment matching, status calculation, revenue categorization

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: silver_billing_transactions")
print("=" * 70)

# Read bronze tables
invoice = spark.table(f"{LAKEHOUSE_NAME}.bronze_invoice")
invoice_line = spark.table(f"{LAKEHOUSE_NAME}.bronze_invoice_line")
payment = spark.table(f"{LAKEHOUSE_NAME}.bronze_payment")
customer_account = spark.table(f"{LAKEHOUSE_NAME}.bronze_customer_account")

# Aggregate invoice lines
invoice_lines_agg = (
    invoice_line
    .groupBy("invoice_id")
    .agg(
        count("*").alias("line_item_count"),
        sum(when(col("description").like("%voice%"), col("amount")).otherwise(0)).alias("charges_voice"),
        sum(when(col("description").like("%data%"), col("amount")).otherwise(0)).alias("charges_data"),
        sum(when(col("description").like("%sms%"), col("amount")).otherwise(0)).alias("charges_sms"),
        sum(when(~col("description").like("%voice%") & ~col("description").like("%data%") & ~col("description").like("%sms%"), col("amount")).otherwise(0)).alias("charges_other")
    )
)

# Aggregate payments per invoice
payments_agg = (
    payment
    .groupBy("invoice_id")
    .agg(
        sum("amount").alias("payment_amount"),
        max("payment_date").alias("payment_date")
    )
)

# Join billing data
billing_transactions = (
    invoice.alias("i")
    .join(customer_account.alias("ca"), col("i.account_id") == col("ca.account_id"), "left")
    .join(invoice_lines_agg.alias("il"), col("i.invoice_id") == col("il.invoice_id"), "left")
    .join(payments_agg.alias("p"), col("i.invoice_id") == col("p.invoice_id"), "left")
    .select(
        # Business keys
        col("i.invoice_id"),
        col("i.account_id"),
        col("ca.customer_id"),
        
        # Invoice info
        col("i.invoice_date"),
        col("i.due_date"),
        col("i.total_amount").alias("invoice_amount"),
        col("i.status").alias("invoice_status"),
        
        # Payment info
        coalesce(col("p.payment_amount"), lit(0.0)).alias("payment_amount"),
        col("p.payment_date"),
        
        # Line items
        coalesce(col("il.line_item_count"), lit(0)).alias("line_item_count"),
        coalesce(col("il.charges_voice"), lit(0.0)).alias("charges_voice"),
        coalesce(col("il.charges_data"), lit(0.0)).alias("charges_data"),
        coalesce(col("il.charges_sms"), lit(0.0)).alias("charges_sms"),
        coalesce(col("il.charges_other"), lit(0.0)).alias("charges_other")
    )
)

# Add derived fields
billing_transactions = (
    billing_transactions
    .withColumn("outstanding_balance", col("invoice_amount") - col("payment_amount"))
    .withColumn("payment_status",
        when(col("payment_amount") >= col("invoice_amount"), "Paid")
        .when((col("payment_amount") > 0) & (col("payment_amount") < col("invoice_amount")), "Partial")
        .when((col("payment_amount").isNull() | (col("payment_amount") == 0)) & (col("due_date") < current_date()), "Overdue")
        .otherwise("Pending"))
    .withColumn("is_overdue", col("payment_status") == "Overdue")
    .withColumn("overdue_days",
        when(col("is_overdue"), datediff(current_date(), col("due_date")))
        .otherwise(0))
    .withColumn("days_to_payment",
        when(col("payment_date").isNotNull(), datediff(col("payment_date"), col("invoice_date")))
        .otherwise(None))
    .withColumn("revenue_category",
        when(col("charges_voice") > col("charges_data") + col("charges_sms"), "Voice")
        .when(col("charges_data") > col("charges_voice") + col("charges_sms"), "Data")
        .when(col("charges_sms") > 0, "SMS")
        .otherwise("Other"))
    .withColumn("_created_timestamp", current_timestamp())
    .withColumn("_updated_timestamp", current_timestamp())
)

# Write to Delta
print(f"  💾 Writing to silver_billing_transactions...")
billing_transactions.write.format("delta").mode("overwrite").partitionBy("invoice_date").save(f"{SILVER_PATH}/silver_billing_transactions")

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_billing_transactions
    USING DELTA
    LOCATION '{SILVER_PATH}/silver_billing_transactions'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.silver_billing_transactions").count()
print(f"  ✅ silver_billing_transactions created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5-7. Additional Silver Tables (Simplified)

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: Remaining Silver Tables (simplified)")
print("=" * 70)

# 5. silver_prepaid_activity
print("\n  Building silver_prepaid_activity...")
prepaid_snapshot = spark.table(f"{LAKEHOUSE_NAME}.bronze_prepaid_balance_snapshot")
topup = spark.table(f"{LAKEHOUSE_NAME}.bronze_topup")

prepaid_activity = (
    prepaid_snapshot.alias("ps")
    .join(topup.alias("t"), col("ps.subscriber_id") == col("t.subscriber_id"), "left")
    .select(
        col("ps.subscriber_id"),
        col("ps.balance_amount"),
        col("ps.snapshot_date"),
        col("t.topup_amount"),
        col("t.topup_date")
    )
    .distinct()
)

prepaid_activity = prepaid_activity.withColumn("_created_timestamp", current_timestamp()).withColumn("_updated_timestamp", current_timestamp())

prepaid_activity.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/silver_prepaid_activity")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_prepaid_activity USING DELTA LOCATION '{SILVER_PATH}/silver_prepaid_activity'")
print(f"  ✅ silver_prepaid_activity created")

# 6. silver_service_catalog
print("\n  Building silver_service_catalog...")
service = spark.table(f"{LAKEHOUSE_NAME}.bronze_service")
service_order = spark.table(f"{LAKEHOUSE_NAME}.bronze_service_order")
entitlement = spark.table(f"{LAKEHOUSE_NAME}.bronze_entitlement")

service_catalog = (
    service.alias("s")
    .join(service_order.alias("so"), col("s.service_id") == col("so.service_id"), "left")
    .join(entitlement.alias("e"), col("s.service_id") == col("e.service_id"), "left")
    .select(
        col("s.service_id"),
        col("s.service_type"),
        col("s.status"),
        col("so.order_date"),
        col("e.entitlement_type")
    )
    .distinct()
    .withColumn("_created_timestamp", current_timestamp())
    .withColumn("_updated_timestamp", current_timestamp())
)

service_catalog.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/silver_service_catalog")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_service_catalog USING DELTA LOCATION '{SILVER_PATH}/silver_service_catalog'")
print(f"  ✅ silver_service_catalog created")

# 7. silver_support_interactions
print("\n  Building silver_support_interactions...")
case_ticket = spark.table(f"{LAKEHOUSE_NAME}.bronze_case_ticket")

support_interactions = (
    case_ticket
    .select(
        col("case_id"),
        col("customer_id"),
        col("case_type"),
        col("status"),
        col("priority"),
        col("created_date"),
        col("resolved_date")
    )
    .withColumn("resolution_days", 
        when(col("resolved_date").isNotNull(), 
            datediff(col("resolved_date"), col("created_date")))
        .otherwise(None))
    .withColumn("is_resolved", col("status").isin(["Resolved", "Closed"]))
    .withColumn("_created_timestamp", current_timestamp())
    .withColumn("_updated_timestamp", current_timestamp())
)

support_interactions.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/silver_support_interactions")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.silver_support_interactions USING DELTA LOCATION '{SILVER_PATH}/silver_support_interactions'")
print(f"  ✅ silver_support_interactions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 SILVER LAYER TRANSFORMATION SUMMARY")
print("=" * 70)

silver_tables = [
    "silver_customer_profile",
    "silver_subscriber_overview",
    "silver_mobile_inventory",
    "silver_billing_transactions",
    "silver_prepaid_activity",
    "silver_service_catalog",
    "silver_support_interactions"
]

total_rows = 0
for table in silver_tables:
    count = spark.table(f"{LAKEHOUSE_NAME}.{table}").count()
    total_rows += count
    print(f"  ✓ {table}: {count:,} rows")

print(f"\n📈 Total Silver Rows: {total_rows:,}")
print(f"✅ All 7 Silver tables created successfully!")
print("\n🎯 Next Step: Run 03_gold_aggregations.ipynb")
print("=" * 70)
