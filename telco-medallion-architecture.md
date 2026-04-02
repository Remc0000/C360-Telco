# Telco Customer 360 - Medallion Architecture Proposal

**Project:** Telco Customer Data Platform - Personalized Insights Engine  
**Workspace:** Telco  
**Lakehouse:** telco_data.Lakehouse  
**Framework:** OpenSpec-driven transformation  
**Goal:** Build a medallion (Bronze → Silver → Gold) architecture to enable highly personalized customer insights from telecommunications data

---

## Executive Summary

The Telco workspace contains 26 CSV files (1.6+ MB) representing a complete telecommunications customer and operational dataset. This proposal outlines a medallion architecture to transform raw data into actionable, personalized insights for customer 360 analysis.

**Data Volume:** 
- **Files:** 26 CSV datasets
- **Total Size:** ~1.6 MB
- **Estimated Rows:** ~5,000-10,000 customers with full lifecycle data
- **Key Entities:** Customers (party), Subscribers, Services, Invoices, Payments, Devices, Support Cases

---

## Data Discovery - Available CSV Files

### **Core Customer & Party Data**
1. `party.csv` (16.9 KB) - Customer master data (PERSON/ORG)
2. `customer_account.csv` (9.4 KB) - Account details
3. `account_party_role.csv` (16.3 KB) - Party-account relationships
4. `address.csv` (15.4 KB) - Address master
5. `party_address.csv` (10.9 KB) - Party-address links

### **Subscriber & Service Data**
6. `subscriber.csv` (19.2 KB) - Mobile subscribers
7. `subscription.csv` (27.2 KB) - Active subscriptions
8. `subscriber_status_history.csv` (27.3 KB) - Status changes
9. `service.csv` (24.9 KB) - Services provisioned
10. `service_order.csv` (54.1 KB) - Service order requests
11. `entitlement.csv` (34.9 KB) - Service entitlements

### **Mobile Assets & Connectivity**
12. `msisdn.csv` (10.4 KB) - Phone numbers
13. `subscriber_msisdn.csv` (9.6 KB) - Subscriber-MSISDN mapping
14. `sim.csv` (13.0 KB) - SIM cards
15. `subscriber_sim.csv` (9.7 KB) - Subscriber-SIM association
16. `device.csv` (13.4 KB) - Mobile devices
17. `subscriber_device.csv` (10.2 KB) - Subscriber-device link
18. `porting_request.csv` (2.2 KB) - Number porting requests

### **Billing & Revenue Data**
19. `invoice.csv` (166.6 KB) - Customer invoices
20. `invoice_line.csv` (545.2 KB) ⭐ LARGEST - Invoice line items
21. `charge.csv` (485.9 KB) - Usage charges
22. `payment.csv` (107.0 KB) - Payment transactions
23. `prepaid_balance_snapshot.csv` (58.1 KB) - Prepaid balances
24. `topup.csv` (27.2 KB) - Prepaid top-ups

### **Product & Support**
25. `product_catalog.csv` (2.4 KB) - Available products
26. `case_ticket.csv` (28.2 KB) - Customer support cases

---

## Proposed Medallion Architecture

### **BRONZE LAYER - Raw Data Ingestion**
**Objective:** Preserve source data exactly as-is with minimal transformation

#### Tasks:
1. **Ingest all 26 CSV files to Delta tables**
   - Location: `Tables/bronze/{source_file_name}`
   - Format: Delta Lake with schema validation
   - Add metadata: `_ingestion_timestamp`, `_source_file`, `_row_hash`
   - Preserve data types from source

2. **Data Quality Signals**
   - Row counts per table
   - Schema drift detection
   - Duplicate detection
   - Null percentage tracking

**Deliverables:**
- 26 Bronze Delta tables in `telco_data.Lakehouse/Tables/bronze/`
- Data quality dashboard (counts, nulls, duplicates)

---

### **SILVER LAYER - Cleansed & Conformed**
**Objective:** Clean, standardize, and link data for analytical use

#### Data Transformations:

**1. Customer Domain (Silver)**
- **`silver_customer_profile`**
  - Combine: `party`, `customer_account`, `account_party_role`, `address`, `party_address`
  - Create unified customer ID
  - Geocode addresses (latitude/longitude)
  - Customer segments: B2C vs B2B, age cohorts, tenure
  - Current status flags

**2. Subscriber Domain (Silver)**
- **`silver_subscriber_overview`**
  - Merge: `subscriber`, `subscription`, `subscriber_status_history`
  - Create subscriber timeline (activation, suspensions, churn)
  - Calculate subscriber tenure, status
  - Link to customer profile

**3. Mobile Assets (Silver)**
- **`silver_mobile_inventory`**
  - Join: `msisdn`, `sim`, `device`, `subscriber_msisdn`, `subscriber_sim`, `subscriber_device`
  - Active phone number inventory per subscriber
  - Device brand/model intelligence
  - SIM swap detection

**4. Service & Product (Silver)**
- **`silver_service_catalog`**
  - Combine: `service`, `service_order`, `entitlement`, `product_catalog`
  - Active services per subscriber
  - Service activation timeline
  - Entitlement coverage matrix

**5. Billing & Revenue (Silver)**
- **`silver_billing_transactions`**
  - Join: `invoice`, `invoice_line`, `charge`, `payment`
  - Complete billing cycle view
  - Payment status (paid, pending, overdue)
  - Revenue attribution by service

**6. Prepaid Management (Silver)**
- **`silver_prepaid_activity`**
  - Merge: `prepaid_balance_snapshot`, `topup`
  - Balance trends over time
  - Top-up frequency patterns
  - Low-balance alerts

**7. Customer Support (Silver)**
- **`silver_support_interactions`**
  - Source: `case_ticket`, `porting_request`
  - Case resolution metrics
  - Reason code clustering
  - Churn risk indicators from support patterns

**Deliverables:**
- 7 Silver-layer dimensional tables
- Data lineage documentation
- SLA metrics (freshness, completeness)

---

### **GOLD LAYER - Analytics-Ready Business Models**
**Objective:** Create high-value aggregated datasets for customer personalization

#### Gold Tables for Personalization:

**1. `gold_customer_360`**
**Purpose:** Complete customer profile for 360° view

**Columns:**
- `customer_id`, `customer_name`, `customer_type` (B2C/B2B)
- `subscriber_count`, `active_services`, `total_devices`
- `tenure_months`, `onboarding_date`, `last_activity_date`
- `lifetime_value`, `avg_monthly_spend`, `payment_behavior_score`
- `support_case_count`, `avg_resolution_days`
- `churn_risk_score`, `churn_risk_flag`
- `preferred_contact_channel`, `email`, `address`
- `segment` (VIP, Standard, At-Risk, Dormant)

**Use Case:** Customer service reps instant 360° view

---

**2. `gold_subscriber_health`**
**Purpose:** Subscriber-level engagement and health metrics

**Columns:**
- `subscriber_id`, `customer_id`, `msisdn`
- `subscription_status`, `status_since_date`, `days_since_last_topup`
- `balance_30day_avg`, `topup_frequency_score`
- `data_usage_trend`, `voice_usage_trend`
- `device_age_months`, `device_upgrade_eligible`
- `payment_punctuality_score`, `overdue_days`
- `support_tickets_90d`, `porting_requested_flag`
- `engagement_score`, `health_status` (Healthy, Warning, Critical)

**Use Case:** Proactive retention campaigns

---

**3. `gold_billing_summary`**
**Purpose:** Revenue analytics and billing insights

**Columns:**
- `customer_id`, `billing_month`
- `total_billed`, `total_paid`, `outstanding_balance`
- `invoice_count`, `line_item_count`
- `avg_charge_per_line`, `payment_on_time_flag`
- `days_to_payment_avg`, `refund_count`
- `revenue_by_service_type` (Voice, Data, SMS, International)
- `arpu` (Average Revenue Per User)
- `billing_consistency_score`

**Use Case:** Revenue forecasting, payment risk modeling

---

**4. `gold_product_affinity`**
**Purpose:** Product usage patterns and cross-sell opportunities

**Columns:**
- `customer_id`, `product_category`
- `active_products`, `product_adoption_date`
- `service_utilization_%`, `unused_entitlements`
- `upgrade_eligible_flag`, `recommended_products`
- `bundle_coverage_gap`
- `product_satisfaction_score` (inferred from support cases)

**Use Case:** Personalized product recommendations

---

**5. `gold_support_analytics`**
**Purpose:** Customer experience and support insights

**Columns:**
- `customer_id`, `ticket_count_total`, `open_tickets`
- `avg_resolution_time_hours`, `first_contact_resolution_%`
- `top_issue_category`, `repeat_issue_flag`
- `escalation_count`, `satisfaction_proxy_score`
- `last_contact_date`, `contact_frequency_trend`
- `support_channel_preference`

**Use Case:** Service improvement, proactive support

---

**6. `gold_churn_risk_model`**
**Purpose:** ML-ready churn prediction features

**Columns:**
- `customer_id`, `churn_probability_score` (0-1)
- `churn_risk_tier` (Low, Medium, High, Critical)
- `contributing_factors` (JSON array: payment_issues, low_usage, support_dissatisfaction)
- `days_since_last_interaction`, `declining_usage_flag`
- `contract_end_date`, `out_of_contract_flag`
- `competitor_porting_flag`, `retention_offer_sent_flag`
- `recommended_retention_action`

**Use Case:** Automated churn prevention workflows

---

**7. `gold_customer_journey_events`**
**Purpose:** Event-driven customer timeline for personalization

**Columns:**
- `customer_id`, `event_timestamp`, `event_type`
- `event_category` (Onboarding, Purchase, Payment, Support, Usage, Churn)
- `event_details` (JSON)
- `channel`, `triggered_campaign`, `outcome`
- `next_best_action`, `time_to_next_event_predicted`

**Use Case:** Journey orchestration, next-best-action engines

---

**8. `gold_kpi_dashboard`**
**Purpose:** Executive KPIs and operational metrics

**Metrics:**
- Total customers, active subscribers, churn rate
- ARPU, total MRR, payment collection rate
- NPS proxy score, support resolution rate
- Service adoption rates, inventory utilization

**Granularity:** Daily, weekly, monthly snapshots

**Use Case:** Executive dashboards, business intelligence

---

## Personalization Use Cases Enabled

### 1. **Real-Time Customer Service**
- Customer calls → Agent instantly sees `gold_customer_360`
- View: Tenure, payment status, open tickets, churn risk
- Action: Personalized retention offer if at-risk

### 2. **Proactive Retention Campaigns**
- Daily scan of `gold_churn_risk_model`
- Trigger: Send SMS/email with special offer when `churn_risk_tier = High`
- Personalize: Offer based on `contributing_factors`

### 3. **Personalized Product Recommendations**
- Use `gold_product_affinity` to find coverage gaps
- Recommend: Family plan if multiple subscribers, international package if frequent roaming
- Deliver: Via app notification or next invoice

### 4. **Payment Default Prevention**
- Monitor `gold_billing_summary` for overdue patterns
- Trigger: Auto-reminder 3 days before payment due
- Personalize: Flexible payment plan for customers with good history

### 5. **Device Upgrade Campaigns**
- Query `gold_subscriber_health` for `device_upgrade_eligible = True`
- Filter: High-value customers with tenure > 24 months
- Offer: Trade-in program with device recommendations

### 6. **Customer Journey Analytics**
- Analyze `gold_customer_journey_events` to identify drop-off points
- Optimize: Onboarding flow, payment process, support touchpoints
- Personalize: Next-best-action recommendations

---

## Technical Implementation Plan

### Phase 1: Bronze Layer (Week 1)
- [ ] Create Fabric notebook: `01_bronze_ingestion.ipynb`
- [ ] Ingest all 26 CSVs to Delta format
- [ ] Add audit columns: `_ingestion_ts`, `_source_file`
- [ ] Create bronze data quality checks
- [ ] Schedule: Daily incremental load (if source updates)

### Phase 2: Silver Layer (Weeks 2-3)
- [ ] Create Fabric notebook: `02_silver_transformations.ipynb`
- [ ] Build 7 silver tables with business logic
- [ ] Implement SCD Type 2 for historical tracking
- [ ] Add data quality rules (uniqueness, referential integrity)
- [ ] Schedule: Daily refresh cascading from bronze

### Phase 3: Gold Layer (Weeks 3-4)
- [ ] Create Fabric notebook: `03_gold_aggregations.ipynb`
- [ ] Build 8 gold analytical tables
- [ ] Implement churn prediction model (ML feature engineering)
- [ ] Create composite scores (engagement, health, risk)
- [ ] Schedule: Hourly/daily refresh based on SLA

### Phase 4: Semantic Layer & Reporting (Week 5)
- [ ] Create Direct Lake semantic model in Power BI
- [ ] Build customer 360 report
- [ ] Build churn analytics dashboard
- [ ] Build revenue & billing dashboard
- [ ] Publish to Fabric workspace

### Phase 5: Operationalization (Week 6)
- [ ] Create Fabric data pipeline for orchestration
- [ ] Implement incremental processing
- [ ] Set up monitoring & alerting (Spark job failures, data quality)
- [ ] Document data lineage & business glossary
- [ ] Create user documentation

---

## Data Quality & Governance

### Quality Checks
1. **Row counts:** Bronze = Silver = Gold (after joins)
2. **Null rates:** < 5% on critical columns (customer_id, msisdn)
3. **Uniqueness:** Primary key constraints validated
4. **Referential integrity:** Foreign keys validated
5. **Freshness:** Data no older than 24 hours

### Governance
- **Ownership:** Data Engineering team
- **Stewardship:** Business Analysts (Telco domain)
- **Access:** Role-based (Gold tables = self-service; Silver/Bronze = restricted)
- **Retention:** Bronze (2 years), Silver (5 years), Gold (7 years)
- **Privacy:** PII masking for non-production environments

---

## Success Metrics

1. **Technical:**
   - Pipeline success rate > 99%
   - Data freshness < 1 hour
   - Query performance: P95 < 5 seconds on Gold tables

2. **Business:**
   - Reduce churn by 15% through proactive campaigns
   - Increase ARPU by 10% via personalized upsells
   - Improve customer satisfaction (NPS +5 points)
   - Reduce support costs by 20% through self-service insights

---

## Next Steps for OpenSpec

Please analyze the following with OpenSpec tools:

1. **Schema Profiling:**
   - Generate detailed schema for all 26 CSV files
   - Identify primary keys and foreign key relationships
   - Detect data quality issues (nulls, duplicates, outliers)

2. **Relationship Mapping:**
   - Create entity-relationship diagram from CSV files
   - Validate join paths for Silver layer consolidation
   - Identify orphaned records

3. **Transformation Logic:**
   - Generate PySpark transformation code for Bronze → Silver
   - Create aggregation queries for Silver → Gold
   - Implement churn risk scoring algorithm

4. **Data Catalog:**
   - Auto-generate business glossary
   - Document column lineage
   - Create sample queries for common use cases

---

## Questions for Business Stakeholders

1. What is the **refresh frequency** requirement? (Real-time, hourly, daily?)
2. Are there specific **customer segments** we should prioritize?
3. What is the **primary use case**? (Retention, upsell, service quality, compliance?)
4. Do we need **historical snapshots** or only current state?
5. What are the **privacy/regulatory requirements** (GDPR, data masking)?

---

## Conclusion

This medallion architecture transforms 26 raw CSV files into a powerful **Customer 360 platform** enabling:
- **Personalized customer experiences** through gold-layer insights
- **Proactive interventions** via churn prediction and health monitoring
- **Revenue optimization** through targeted campaigns and product recommendations
- **Operational efficiency** via self-service analytics on gold tables

**Estimated Effort:** 6 weeks (1 FTE data engineer + 0.5 FTE analyst)  
**Expected ROI:** 3-6 months through churn reduction and ARPU increase

---

**Generated for:** OpenSpec Framework  
**Date:** 2026-04-02  
**Contact:** Data Engineering Team
