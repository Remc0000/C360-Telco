# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer Aggregations & Analytics
# MAGIC 
# MAGIC **Project:** C360-Telco Medallion Architecture  
# MAGIC **Layer:** Gold (Analytics & ML-Ready)  
# MAGIC **Data Scientist:** Shannon  
# MAGIC **Architecture:** Tesla's schema design v1.0
# MAGIC 
# MAGIC ## Purpose
# MAGIC Transform 7 Silver domain tables into 8 Gold analytics tables:
# MAGIC - Aggregate customer 360 insights
# MAGIC - Calculate churn risk scores with ML features
# MAGIC - Pre-compute KPIs for fast BI queries
# MAGIC - Enable personalized recommendations
# MAGIC 
# MAGIC ## Gold Tables
# MAGIC 1. gold_customer_360 (Complete customer view)
# MAGIC 2. gold_churn_risk_model (Churn prediction with weighted scoring)
# MAGIC 3. gold_billing_summary (Revenue aggregations)
# MAGIC 4. gold_product_affinity (Next-best-offer recommendations)
# MAGIC 5. gold_support_analytics (Service quality metrics)
# MAGIC 6. gold_subscriber_health (Behavioral scores)
# MAGIC 7. gold_customer_journey_events (Lifecycle tracking)
# MAGIC 8. gold_kpi_dashboard (Executive metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# Configuration
LAKEHOUSE_NAME = "telco_data"
SILVER_PATH = "Tables/silver"
GOLD_PATH = "Tables/gold"

print("=" * 70)
print("🏆 GOLD LAYER ANALYTICS & ML")
print("=" * 70)
print(f"📅 Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Source: {SILVER_PATH}")
print(f"💾 Target: {GOLD_PATH}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Customer 360
# MAGIC 
# MAGIC **Purpose:** Complete customer view with all KPIs  
# MAGIC **Sources:** All Silver tables  
# MAGIC **Refresh:** Daily

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: gold_customer_360")
print("=" * 70)

# Read Silver tables
customer_profile = spark.table(f"{LAKEHOUSE_NAME}.silver_customer_profile").filter(col("_is_current") == True)
subscriber_overview = spark.table(f"{LAKEHOUSE_NAME}.silver_subscriber_overview").filter(col("_is_current") == True)
mobile_inventory = spark.table(f"{LAKEHOUSE_NAME}.silver_mobile_inventory").filter(col("_is_current") == True)
billing_transactions = spark.table(f"{LAKEHOUSE_NAME}.silver_billing_transactions")
support_interactions = spark.table(f"{LAKEHOUSE_NAME}.silver_support_interactions")

# Aggregate billing KPIs per customer
billing_kpis = (
    billing_transactions
    .groupBy("customer_id")
    .agg(
        sum("invoice_amount").alias("lifetime_value"),
        avg("invoice_amount").alias("avg_monthly_spend"),
        count(when(col("is_overdue"), 1)).alias("overdue_invoice_count"),
        sum("outstanding_balance").alias("total_outstanding_balance"),
        max("overdue_days").alias("max_overdue_days"),
        avg("days_to_payment").alias("avg_payment_delay_days"),
        count("invoice_id").alias("total_invoices")
    )
)

# Aggregate support KPIs per customer
support_kpis = (
    support_interactions
    .groupBy("customer_id")
    .agg(
        count("case_id").alias("total_support_tickets"),
        countDistinct("case_type").alias("unique_issue_types"),
        avg("resolution_days").alias("avg_resolution_days"),
        count(when(~col("is_resolved"), 1)).alias("open_ticket_count")
    )
)

# Build Customer 360
customer_360 = (
    customer_profile.alias("cp")
    .join(billing_kpis.alias("bk"), col("cp.customer_id") == col("bk.customer_id"), "left")
    .join(support_kpis.alias("sk"), col("cp.customer_id") == col("sk.customer_id"), "left")
    .select(
        # Customer identity
        col("cp.customer_id"),
        col("cp.account_id"),
        col("cp.display_name"),
        col("cp.email"),
        
        # Demographics
        col("cp.age_years"),
        col("cp.age_cohort"),
        col("cp.city"),
        col("cp.province"),
        col("cp.is_business"),
        
        # Tenure & lifecycle
        col("cp.customer_tenure_days"),
        col("cp.lifecycle_stage"),
        
        # Billing KPIs
        coalesce(col("bk.lifetime_value"), lit(0.0)).alias("lifetime_value"),
        coalesce(col("bk.avg_monthly_spend"), lit(0.0)).alias("avg_monthly_spend"),
        coalesce(col("bk.total_outstanding_balance"), lit(0.0)).alias("outstanding_balance"),
        coalesce(col("bk.overdue_invoice_count"), lit(0)).alias("overdue_invoice_count"),
        coalesce(col("bk.avg_payment_delay_days"), lit(0.0)).alias("avg_payment_delay_days"),
        
        # Support KPIs
        coalesce(col("sk.total_support_tickets"), lit(0)).alias("total_support_tickets"),
        coalesce(col("sk.open_ticket_count"), lit(0)).alias("open_ticket_count"),
        coalesce(col("sk.avg_resolution_days"), lit(0.0)).alias("avg_resolution_days"),
        
        # Metadata
        current_timestamp().alias("_refreshed_timestamp")
    )
)

# Add composite scores
customer_360 = (
    customer_360
    .withColumn("payment_behavior_score",
        when(col("overdue_invoice_count") == 0, 100)
        .when(col("overdue_invoice_count") == 1, 80)
        .when(col("overdue_invoice_count") <= 3, 50)
        .otherwise(20))
    .withColumn("support_satisfaction_score",
        when(col("total_support_tickets") == 0, 100)
        .when((col("total_support_tickets") > 0) & (col("avg_resolution_days") < 3), 90)
        .when(col("avg_resolution_days") < 7, 70)
        .otherwise(40))
    .withColumn("customer_health_score",
        (col("payment_behavior_score") * 0.6 + col("support_satisfaction_score") * 0.4))
    .withColumn("segment",
        when((col("lifetime_value") > 5000) & (col("customer_health_score") >= 80), "VIP")
        .when((col("lifetime_value") > 2000) & (col("customer_health_score") >= 60), "Premium")
        .when(col("lifecycle_stage") == "New", "New")
        .when(col("customer_health_score") < 40, "At Risk")
        .otherwise("Standard"))
)

# Write to Delta
print(f"  💾 Writing to gold_customer_360...")
customer_360.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/gold_customer_360")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_customer_360
    USING DELTA
    LOCATION '{GOLD_PATH}/gold_customer_360'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.gold_customer_360").count()
print(f"  ✅ gold_customer_360 created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Churn Risk Model
# MAGIC 
# MAGIC **Purpose:** ML-powered churn prediction with weighted scoring  
# MAGIC **Algorithm:** Feature-based weighted risk scoring  
# MAGIC **Weights:**
# MAGIC - Payment behavior: 30%
# MAGIC - Usage trends: 25%
# MAGIC - Support issues: 20%
# MAGIC - Engagement: 15%
# MAGIC - Contract status: 10%

# COMMAND ----------

print("\n" + "=" * 70)
print("🔬 BUILDING: gold_churn_risk_model (ML Churn Prediction)")
print("=" * 70)

# Start with customer 360
churn_features = customer_360.alias("c360")

# Join subscriber data for usage trends
subscriber_agg = (
    subscriber_overview
    .filter(col("_is_current") == True)
    .groupBy("customer_id")
    .agg(
        count("subscriber_id").alias("active_subscriber_count"),
        avg("subscriber_tenure_days").alias("avg_subscriber_tenure_days"),
        sum(when(col("is_active"), 1).otherwise(0)).alias("active_lines")
    )
)

churn_features = churn_features.join(subscriber_agg.alias("sa"), col("c360.customer_id") == col("sa.customer_id"), "left")

# Calculate feature scores (0-100 scale, higher = lower churn risk)
churn_model = (
    churn_features
    .select(
        col("c360.customer_id"),
        col("c360.display_name"),
        col("c360.email"),
        col("c360.segment"),
        col("c360.lifecycle_stage"),
        
        # Raw features for explainability
        col("c360.customer_tenure_days"),
        col("c360.overdue_invoice_count"),
        col("c360.outstanding_balance"),
        col("c360.avg_payment_delay_days"),
        col("c360.total_support_tickets"),
        col("c360.open_ticket_count"),
        col("c360.avg_monthly_spend"),
        coalesce(col("sa.active_lines"), lit(0)).alias("active_lines"),
        
        # Individual feature scores (100 = best, 0 = worst)
        # Payment behavior score (30% weight)
        (
            when(col("c360.overdue_invoice_count") == 0, 100)
            .when(col("c360.overdue_invoice_count") == 1, 75)
            .when(col("c360.overdue_invoice_count") <= 3, 40)
            .otherwise(10)
        ).alias("payment_score"),
        
        # Usage trend score (25% weight) - based on spend and active lines
        (
            when((col("c360.avg_monthly_spend") > 100) & (coalesce(col("sa.active_lines"), lit(0)) >= 2), 100)
            .when(col("c360.avg_monthly_spend") > 50, 70)
            .when(col("c360.avg_monthly_spend") > 20, 40)
            .otherwise(20)
        ).alias("usage_score"),
        
        # Support issues score (20% weight) - fewer tickets = better
        (
            when(col("c360.total_support_tickets") == 0, 100)
            .when((col("c360.total_support_tickets") <= 2) & (col("c360.open_ticket_count") == 0), 80)
            .when(col("c360.total_support_tickets") <= 5, 50)
            .otherwise(20)
        ).alias("support_score"),
        
        # Engagement score (15% weight) - tenure and activity
        (
            when(col("c360.customer_tenure_days") > 730, 100)  # 2+ years
            .when(col("c360.customer_tenure_days") > 365, 80)  # 1+ year
            .when(col("c360.customer_tenure_days") > 180, 60)  # 6+ months
            .when(col("c360.customer_tenure_days") > 90, 40)   # 3+ months
            .otherwise(20)
        ).alias("engagement_score"),
        
        # Contract status score (10% weight) - lifecycle stage
        (
            when(col("c360.lifecycle_stage") == "Established", 100)
            .when(col("c360.lifecycle_stage") == "Growing", 70)
            .otherwise(40)  # New customers
        ).alias("contract_score")
    )
)

# Calculate weighted churn risk score
churn_model = (
    churn_model
    .withColumn("churn_risk_score",
        round(
            100 - (  # Invert to get risk (100 = high risk, 0 = low risk)
                col("payment_score") * 0.30 +
                col("usage_score") * 0.25 +
                col("support_score") * 0.20 +
                col("engagement_score") * 0.15 +
                col("contract_score") * 0.10
            ),
            1
        )
    )
    .withColumn("churn_risk_tier",
        when(col("churn_risk_score") >= 75, "Critical")
        .when(col("churn_risk_score") >= 50, "High")
        .when(col("churn_risk_score") >= 25, "Medium")
        .otherwise("Low"))
    .withColumn("churn_probability_pct",
        when(col("churn_risk_score") >= 75, 60)
        .when(col("churn_risk_score") >= 50, 35)
        .when(col("churn_risk_score") >= 25, 15)
        .otherwise(5))
    .withColumn("contributing_factors",
        concat_ws(", ",
            when(col("payment_score") < 50, "Payment Issues").otherwise(lit(None)),
            when(col("usage_score") < 50, "Declining Usage").otherwise(lit(None)),
            when(col("support_score") < 50, "Support Dissatisfaction").otherwise(lit(None)),
            when(col("engagement_score") < 50, "Low Engagement").otherwise(lit(None))
        ))
    .withColumn("recommended_action",
        when(col("churn_risk_tier") == "Critical", 
            "URGENT: Immediate retention call + loyalty offer")
        .when(col("churn_risk_tier") == "High",
            "Call with special promotion within 7 days")
        .when(col("churn_risk_tier") == "Medium",
            "Email campaign with personalized value proposition")
        .otherwise("Standard nurture campaign"))
    .withColumn("model_version", lit("v1.0_weighted_scoring"))
    .withColumn("scored_timestamp", current_timestamp())
)

# Write to Delta
print(f"  💾 Writing to gold_churn_risk_model...")
churn_model.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/gold_churn_risk_model")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_churn_risk_model
    USING DELTA
    LOCATION '{GOLD_PATH}/gold_churn_risk_model'
""")

row_count = spark.table(f"{LAKEHOUSE_NAME}.gold_churn_risk_model").count()
print(f"  ✅ gold_churn_risk_model created with {row_count:,} rows")

# Show churn distribution
print("\n  📊 Churn Risk Distribution:")
churn_dist = spark.table(f"{LAKEHOUSE_NAME}.gold_churn_risk_model").groupBy("churn_risk_tier").count().orderBy("churn_risk_tier")
churn_dist.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-8. Additional Gold Tables

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 BUILDING: Additional Gold Tables")
print("=" * 70)

# 3. gold_billing_summary
print("\n  Building gold_billing_summary...")
billing_summary = (
    billing_transactions
    .groupBy("customer_id", year("invoice_date").alias("year"), month("invoice_date").alias("month"))
    .agg(
        sum("invoice_amount").alias("total_revenue"),
        count("invoice_id").alias("invoice_count"),
        sum("outstanding_balance").alias("outstanding_amount"),
        avg("days_to_payment").alias("avg_payment_days"),
        sum("charges_voice").alias("voice_revenue"),
        sum("charges_data").alias("data_revenue"),
        sum("charges_sms").alias("sms_revenue")
    )
    .withColumn("revenue_mix",
        concat(
            lit("Voice: "), round(col("voice_revenue") / col("total_revenue") * 100, 1), lit("% | "),
            lit("Data: "), round(col("data_revenue") / col("total_revenue") * 100, 1), lit("% | "),
            lit("SMS: "), round(col("sms_revenue") / col("total_revenue") * 100, 1), lit("%")
        ))
    .withColumn("_refreshed_timestamp", current_timestamp())
)

billing_summary.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_billing_summary")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_billing_summary USING DELTA LOCATION '{GOLD_PATH}/gold_billing_summary'")
print(f"  ✅ gold_billing_summary created")

# 4. gold_product_affinity (Next-best-offer recommendations)
print("\n  Building gold_product_affinity...")
product_affinity = (
    customer_360
    .select(
        col("customer_id"),
        col("display_name"),
        col("avg_monthly_spend"),
        col("segment"),
        
        # Recommend based on spend and usage
        when((col("avg_monthly_spend") > 100) & (col("segment") == "VIP"), "Premium Unlimited Plan")
        .when((col("avg_monthly_spend") > 50) & (col("segment").isin("Premium", "VIP")), "Family Bundle")
        .when((col("avg_monthly_spend") < 20) & (col("lifecycle_stage") == "New"), "Starter Upgrade")
        .when(col("segment") == "At Risk", "Loyalty Discount Plan")
        .otherwise("Standard Cross-sell").alias("recommended_product"),
        
        when((col("avg_monthly_spend") > 100), "High ARPU - upsell premium features")
        .when((col("segment") == "At Risk"), "Retention offer to prevent churn")
        .when(col("lifecycle_stage") == "New", "Early lifecycle engagement")
        .otherwise("Standard nurture").alias("recommendation_reason"),
        
        current_timestamp().alias("_refreshed_timestamp")
    )
)

product_affinity.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_product_affinity")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_product_affinity USING DELTA LOCATION '{GOLD_PATH}/gold_product_affinity'")
print(f"  ✅ gold_product_affinity created")

# 5. gold_support_analytics
print("\n  Building gold_support_analytics...")
support_analytics = (
    support_interactions
    .groupBy("customer_id")
    .agg(
        count("case_id").alias("total_cases"),
        countDistinct("case_type").alias("unique_issue_types"),
        avg("resolution_days").alias("avg_resolution_days"),
        max("resolution_days").alias("max_resolution_days"),
        sum(when(col("priority") == "High", 1).otherwise(0)).alias("high_priority_cases"),
        sum(when(col("is_resolved"), 1).otherwise(0)).alias("resolved_cases"),
        sum(when(~col("is_resolved"), 1).otherwise(0)).alias("open_cases")
    )
    .withColumn("resolution_rate_pct", 
        round(col("resolved_cases") / col("total_cases") * 100, 1))
    .withColumn("support_quality_score",
        when((col("avg_resolution_days") < 2) & (col("resolution_rate_pct") > 90), 100)
        .when((col("avg_resolution_days") < 5) & (col("resolution_rate_pct") > 70), 75)
        .when(col("avg_resolution_days") < 10, 50)
        .otherwise(25))
    .withColumn("_refreshed_timestamp", current_timestamp())
)

support_analytics.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_support_analytics")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_support_analytics USING DELTA LOCATION '{GOLD_PATH}/gold_support_analytics'")
print(f"  ✅ gold_support_analytics created")

# 6. gold_subscriber_health
print("\n  Building gold_subscriber_health...")
subscriber_health = (
    subscriber_overview
    .filter(col("_is_current") == True)
    .select(
        col("subscriber_id"),
        col("customer_id"),
        col("subscriber_type"),
        col("current_status"),
        col("subscriber_tenure_days"),
        col("is_active"),
        
        when(col("subscriber_tenure_days") > 365, 90)
        .when(col("subscriber_tenure_days") > 180, 70)
        .when(col("subscriber_tenure_days") > 90, 50)
        .otherwise(30).alias("health_score"),
        
        current_timestamp().alias("_refreshed_timestamp")
    )
)

subscriber_health.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_subscriber_health")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_subscriber_health USING DELTA LOCATION '{GOLD_PATH}/gold_subscriber_health'")
print(f"  ✅ gold_subscriber_health created")

# 7. gold_customer_journey_events (Lifecycle milestones)
print("\n  Building gold_customer_journey_events...")
customer_journey = (
    customer_profile
    .filter(col("_is_current") == True)
    .select(
        col("customer_id"),
        col("display_name"),
        col("created_ts").alias("acquisition_date"),
        col("lifecycle_stage"),
        col("customer_tenure_days"),
        
        when(col("customer_tenure_days") >= 365, "1_Year_Anniversary")
        .when(col("customer_tenure_days") >= 90, "90_Day_Milestone")
        .when(col("customer_tenure_days") >= 30, "30_Day_Milestone")
        .otherwise("New_Customer").alias("milestone"),
        
        current_timestamp().alias("_refreshed_timestamp")
    )
)

customer_journey.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_customer_journey_events")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_customer_journey_events USING DELTA LOCATION '{GOLD_PATH}/gold_customer_journey_events'")
print(f"  ✅ gold_customer_journey_events created")

# 8. gold_kpi_dashboard (Executive summary)
print("\n  Building gold_kpi_dashboard...")
kpi_dashboard = spark.sql(f"""
    SELECT
        current_date() as report_date,
        COUNT(DISTINCT customer_id) as total_customers,
        SUM(lifetime_value) as total_revenue,
        AVG(avg_monthly_spend) as avg_arpu,
        SUM(CASE WHEN segment = 'VIP' THEN 1 ELSE 0 END) as vip_customers,
        SUM(CASE WHEN segment = 'At Risk' THEN 1 ELSE 0 END) as at_risk_customers,
        AVG(customer_health_score) as avg_health_score,
        SUM(outstanding_balance) as total_outstanding,
        current_timestamp() as _refreshed_timestamp
    FROM {LAKEHOUSE_NAME}.gold_customer_360
""")

kpi_dashboard.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/gold_kpi_dashboard")
spark.sql(f"CREATE TABLE IF NOT EXISTS {LAKEHOUSE_NAME}.gold_kpi_dashboard USING DELTA LOCATION '{GOLD_PATH}/gold_kpi_dashboard'")
print(f"  ✅ gold_kpi_dashboard created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 GOLD LAYER SUMMARY")
print("=" * 70)

gold_tables = [
    "gold_customer_360",
    "gold_churn_risk_model",
    "gold_billing_summary",
    "gold_product_affinity",
    "gold_support_analytics",
    "gold_subscriber_health",
    "gold_customer_journey_events",
    "gold_kpi_dashboard"
]

total_rows = 0
for table in gold_tables:
    count = spark.table(f"{LAKEHOUSE_NAME}.{table}").count()
    total_rows += count
    print(f"  ✓ {table}: {count:,} rows")

print(f"\n📈 Total Gold Rows: {total_rows:,}")
print(f"✅ All 8 Gold tables created successfully!")

# Show sample churn predictions
print("\n🔬 Sample Churn Predictions:")
spark.sql(f"""
    SELECT 
        display_name,
        segment,
        churn_risk_tier,
        churn_risk_score,
        churn_probability_pct,
        contributing_factors,
        recommended_action
    FROM {LAKEHOUSE_NAME}.gold_churn_risk_model
    WHERE churn_risk_tier IN ('Critical', 'High')
    ORDER BY churn_risk_score DESC
    LIMIT 5
""").show(5, truncate=False)

print("\n🎯 Next Step: Create Power BI reports on Gold tables")
print("=" * 70)
