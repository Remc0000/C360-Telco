# Telco Medallion Architecture - Schema Design
**Architect:** Tesla  
**Date:** April 2, 2026  
**Status:** Phase 1 - Bronze & Silver Layer Design

---

## Bronze Layer Schema (26 Tables)

### Design Principles
1. **Preserve Source Fidelity:** Keep all original columns from CSV files
2. **Add Audit Columns:** Track lineage and processing metadata
3. **Delta Lake Format:** Use for ACID transactions and time travel
4. **No Transformations:** Raw data preservation only

### Standard Audit Columns (All Bronze Tables)
```python
_ingestion_timestamp TIMESTAMP  # When record was loaded
_source_file STRING            # Original CSV filename
_row_hash STRING              # MD5 hash of row for deduplication
_is_current BOOLEAN           # For incremental loads (default TRUE)
_processing_date DATE         # Partition key
```

---

## Bronze Table Definitions

### 1. bronze_party (Customer Master)
**Purpose:** Core customer/organization data  
**Source:** party.csv (16.9 KB, ~300 rows)

```sql
CREATE TABLE bronze_party (
    party_id STRING PRIMARY KEY,
    party_type STRING,              -- PERSON or ORG
    display_name STRING,
    legal_name STRING,
    given_name STRING,
    family_name STRING,
    birth_date DATE,
    email STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    -- Audit columns
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date)
LOCATION 'Tables/bronze/bronze_party'
```

### 2. bronze_customer_account
**Purpose:** Account information  
**Source:** customer_account.csv (9.4 KB)

```sql
CREATE TABLE bronze_customer_account (
    account_id STRING PRIMARY KEY,
    customer_id STRING,  -- FK to party_id
    account_type STRING,
    account_status STRING,
    billing_cycle STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date)
LOCATION 'Tables/bronze/bronze_customer_account'
```

### 3. bronze_address
**Purpose:** Address master data  
**Source:** address.csv (15.4 KB)

```sql
CREATE TABLE bronze_address (
    address_id STRING PRIMARY KEY,
    street_address STRING,
    house_number STRING,
    postal_code STRING,
    city STRING,
    province STRING,
    country STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
LOCATION 'Tables/bronze/bronze_address'
```

### 4. bronze_subscriber
**Purpose:** Mobile subscriber data  
**Source:** subscriber.csv (19.2 KB)

```sql
CREATE TABLE bronze_subscriber (
    subscriber_id STRING PRIMARY KEY,
    customer_id STRING,  -- FK to party_id
    subscriber_type STRING,
    status STRING,
    activation_date DATE,
    created_ts TIMESTAMP,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date)
LOCATION 'Tables/bronze/bronze_subscriber'
```

### 5. bronze_subscription
**Purpose:** Active subscriptions  
**Source:** subscription.csv (27.2 KB)

```sql
CREATE TABLE bronze_subscription (
    subscription_id STRING PRIMARY KEY,
    subscriber_id STRING,  -- FK
    product_id STRING,
    status STRING,
    activation_date DATE,
    expiry_date DATE,
    auto_renew BOOLEAN,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date)
LOCATION 'Tables/bronze/bronze_subscription'
```

### 6. bronze_invoice
**Purpose:** Customer invoices  
**Source:** invoice.csv (166.6 KB) - High volume

```sql
CREATE TABLE bronze_invoice (
    invoice_id STRING PRIMARY KEY,
    account_id STRING,  -- FK
    invoice_date DATE,
    due_date DATE,
    total_amount DECIMAL(10,2),
    currency STRING,
    status STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date, invoice_date)
LOCATION 'Tables/bronze/bronze_invoice'
```

### 7. bronze_invoice_line
**Purpose:** Invoice line items  
**Source:** invoice_line.csv (545.2 KB) - **LARGEST FILE**

```sql
CREATE TABLE bronze_invoice_line (
    invoice_line_id STRING PRIMARY KEY,
    invoice_id STRING,  -- FK
    line_number INT,
    description STRING,
    quantity DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    amount DECIMAL(10,2),
    service_id STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _row_hash STRING,
    _is_current BOOLEAN,
    _processing_date DATE
)
USING DELTA
PARTITIONED BY (_processing_date)
LOCATION 'Tables/bronze/bronze_invoice_line'
```

*[Additional 19 bronze tables follow same pattern...]*

---

## Silver Layer Schema (7 Domain Tables)

### Design Principles
1. **Data Cleansing:** Remove nulls, standardize formats, validate referential integrity
2. **Conformed Dimensions:** Standardized business keys and naming
3. **SCD Type 2:** Track historical changes where needed
4. **Business Logic:** Apply domain-specific transformations

### Standard Silver Columns
```python
-- SCD Type 2 columns
_valid_from TIMESTAMP
_valid_to TIMESTAMP
_is_current BOOLEAN
_version INT

-- Audit columns
_created_timestamp TIMESTAMP
_updated_timestamp TIMESTAMP
```

---

## Silver Table Definitions

### 1. silver_customer_profile (Customer Domain)
**Purpose:** Unified customer 360° profile  
**Sources:** bronze_party + bronze_customer_account + bronze_address + bronze_party_address

```sql
CREATE TABLE silver_customer_profile (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- Surrogate key
    customer_id STRING NOT NULL,  -- Business key
    party_type STRING NOT NULL,
    display_name STRING,
    given_name STRING,
    family_name STRING,
    email STRING,
    birth_date DATE,
    age_years INT,
    age_cohort STRING,  -- '18-25', '26-35', etc.
    -- Account info
    account_id STRING,
    account_type STRING,
    account_status STRING,
    -- Address info
    full_address STRING,
    postal_code STRING,
    city STRING,
    province STRING,
    -- Derived fields
    customer_tenure_days INT,
    lifecycle_stage STRING,  -- New, Growing, Established
    is_business BOOLEAN,
    -- SCD Type 2
    _valid_from TIMESTAMP,
    _valid_to TIMESTAMP,
    _is_current BOOLEAN,
    _version INT,
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (_is_current)
LOCATION 'Tables/silver/silver_customer_profile'
```

### 2. silver_subscriber_overview (Subscriber Domain)
**Purpose:** Subscriber lifecycle and status  
**Sources:** bronze_subscriber + bronze_subscription + bronze_subscriber_status_history

```sql
CREATE TABLE silver_subscriber_overview (
    subscriber_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id STRING NOT NULL,
    customer_id STRING NOT NULL,  -- FK to silver_customer_profile
    subscription_id STRING,
    subscriber_type STRING,
    current_status STRING,
    activation_date DATE,
    subscriber_tenure_days INT,
    status_change_count INT,
    last_status_change_date DATE,
    is_active BOOLEAN,
    -- SCD Type 2
    _valid_from TIMESTAMP,
    _valid_to TIMESTAMP,
    _is_current BOOLEAN,
    _version INT,
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (_is_current, current_status)
LOCATION 'Tables/silver/silver_subscriber_overview'
```

### 3. silver_mobile_inventory (Assets Domain)
**Purpose:** Mobile asset tracking (MSISDN, SIM, devices)  
**Sources:** bronze_msisdn + bronze_sim + bronze_device + subscriber associations

```sql
CREATE TABLE silver_mobile_inventory (
    asset_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id STRING,
    -- Phone number
    msisdn STRING,
    msisdn_status STRING,
    msisdn_assigned_date DATE,
    -- SIM card
    sim_id STRING,
    iccid STRING,
    sim_status STRING,
    sim_assigned_date DATE,
    -- Device
    device_id STRING,
    device_brand STRING,
    device_model STRING,
    device_assigned_date DATE,
    device_age_months INT,
    -- Composite flags
    is_complete_setup BOOLEAN,  -- Has MSISDN + SIM + Device
    sim_swap_count INT,
    -- SCD Type 2
    _valid_from TIMESTAMP,
    _valid_to TIMESTAMP,
    _is_current BOOLEAN,
    _version INT,
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (_is_current)
LOCATION 'Tables/silver/silver_mobile_inventory'
```

### 4. silver_billing_transactions (Revenue Domain)
**Purpose:** Complete billing view  
**Sources:** bronze_invoice + bronze_invoice_line + bronze_charge + bronze_payment

```sql
CREATE TABLE silver_billing_transactions (
    transaction_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    invoice_id STRING,
    account_id STRING,
    customer_id STRING,  -- Denormalized FK
    invoice_date DATE,
    due_date DATE,
    invoice_amount DECIMAL(12,2),
    payment_amount DECIMAL(12,2),
    outstanding_balance DECIMAL(12,2),
    payment_status STRING,  -- Paid, Partial, Overdue, Pending
    payment_date DATE,
    days_to_payment INT,
    is_overdue BOOLEAN,
    overdue_days INT,
    line_item_count INT,
    -- Categorization
    revenue_category STRING,  -- Voice, Data, SMS, International
    charges_voice DECIMAL(10,2),
    charges_data DECIMAL(10,2),
    charges_sms DECIMAL(10,2),
    charges_other DECIMAL(10,2),
    -- Create timestamp
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (invoice_date)
LOCATION 'Tables/silver/silver_billing_transactions'
```

### 5. silver_prepaid_activity
### 6. silver_service_catalog
### 7. silver_support_interactions

*[Additional silver tables follow similar pattern...]*

---

## Gold Layer Schema (8 Analytics Tables)

### Design Principles
1. **Business-Ready:** Aggregated, denormalized for fast queries
2. **KPI-Focused:** Pre-calculated metrics and scores
3. **Optimized for Reporting:** Star schema compatible
4. **Real-time Ready:** Incremental refresh capability

---

## Gold Table Definitions

### 1. gold_customer_360 (Complete customer view)
**Purpose:** Single pane of glass for customer analysis  
**Refresh:** Daily  
**Query Pattern:** Customer lookup by ID, segment analysis

```sql
CREATE TABLE gold_customer_360 (
    customer_id STRING PRIMARY KEY,
    customer_name STRING,
    customer_type STRING,  -- B2C, B2B
    email STRING,
    age_years INT,
    age_cohort STRING,
    city STRING,
    province STRING,
    -- Subscriber metrics
    subscriber_count INT,
    active_subscribers INT,
    -- Service metrics
    active_services INT,
    total_devices INT,
    -- Financial metrics
    lifetime_value DECIMAL(12,2),
    avg_monthly_spend DECIMAL(10,2),
    last_invoice_amount DECIMAL(10,2),
    outstanding_balance DECIMAL(10,2),
    payment_behavior_score DECIMAL(3,2),  -- 0-1 scale
    -- Support metrics
    support_case_count INT,
    open_cases INT,
    avg_resolution_days DECIMAL(5,2),
    -- Churn risk
    churn_risk_score DECIMAL(3,2),  -- 0-1 scale
    churn_risk_tier STRING,  -- Low, Medium, High, Critical
    churn_contributing_factors ARRAY<STRING>,
    -- Lifecycle
    tenure_months INT,
    onboarding_date DATE,
    last_activity_date DATE,
    lifecycle_stage STRING,
    customer_segment STRING,  -- VIP, Standard, At-Risk, Dormant
    -- Timestamps
    snapshot_date DATE,
    _updated_timestamp TIMESTAMP
)
USING DELTA
ZORDER BY (customer_id, customer_segment, churn_risk_tier)
LOCATION 'Tables/gold/gold_customer_360'
```

### 2. gold_churn_risk_model (ML-ready features)
**Purpose:** Churn prediction and prevention  
**Refresh:** Daily  
**ML Integration:** Feature store for churn models

```sql
CREATE TABLE gold_churn_risk_model (
    subscriber_id STRING PRIMARY KEY,
    customer_id STRING,
    score_date DATE,
    -- Churn prediction
    churn_probability DECIMAL(3,2),  -- 0-1 scale
    churn_risk_tier STRING,
    recommended_retention_action STRING,
    -- Feature categories
    payment_score DECIMAL(3,2),  -- Weight: 30%
    usage_score DECIMAL(3,2),    -- Weight: 25%
    support_score DECIMAL(3,2),  -- Weight: 20%
    engagement_score DECIMAL(3,2), -- Weight: 15%
    contract_score DECIMAL(3,2),   -- Weight: 10%
    -- Payment features
    overdue_days INT,
    late_payment_count_6m INT,
    payment_punctuality_score DECIMAL(3,2),
    -- Usage features
    usage_trend_3m DECIMAL(5,2),  -- % change
    data_usage_mb_avg DECIMAL(10,2),
    voice_minutes_avg DECIMAL(8,2),
    days_since_last_topup INT,
    -- Support features
    support_tickets_90d INT,
    unresolved_issues INT,
    escalation_count INT,
    -- Engagement features
    days_since_last_interaction INT,
    app_login_count_30d INT,
    feature_adoption_score DECIMAL(3,2),
    -- Contract features
    out_of_contract BOOLEAN,
    days_until_contract_end INT,
    competitor_porting_requested BOOLEAN,
    -- Timestamps
    _scored_timestamp TIMESTAMP
)
USING DELTA
ZORDER BY (churn_risk_tier, customer_id)
LOCATION 'Tables/gold/gold_churn_risk_model'
```

*[Additional 6 gold tables: billing_summary, product_affinity, support_analytics, subscriber_health, customer_journey_events, kpi_dashboard...]*

---

## Performance Optimization Strategy

### Partitioning
- **Bronze:** By `_processing_date` (daily partitions)
- **Silver:** By `_is_current` and relevant business key
- **Gold:** By date/month where applicable

### Z-ORDER Optimization
```sql
-- Customer 360 - optimize for customer lookup
OPTIMIZE gold_customer_360 ZORDER BY (customer_id, customer_segment);

-- Churn model - optimize for risk tier queries
OPTIMIZE gold_churn_risk_model ZORDER BY (churn_risk_tier, score_date);

-- Billing - optimize for date range queries
OPTIMIZE silver_billing_transactions ZORDER BY (account_id, invoice_date);
```

### Delta Lake Features
- **Time Travel:** 30 days retention on Bronze/Silver
- **VACUUM:** Weekly cleanup of old versions
- **Auto-Optimize:** Enable on Gold tables for query performance

---

## Data Quality Rules

### Bronze Layer
- Row count validation (compare to source CSV)
- Duplicate detection (using _row_hash)
- Schema validation (expected columns present)

### Silver Layer
- Referential integrity (FK validation)
- Null checks on critical fields (< 5% tolerance)
- Data type validation
- Business rule validation

### Gold Layer
- KPI calculation validation
- Score range checks (0-1 for probabilities)
- Completeness checks (all customers represented)

---

**Schema Design Status:** ✅ Phase 1 Complete (Bronze & Silver)  
**Next Phase:** Marconi to implement in notebooks  
**Review:** Pending Bell's approval

*— Tesla, Solution Architect*
