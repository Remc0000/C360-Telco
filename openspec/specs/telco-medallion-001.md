---
id: telco-medallion-001
title: Telco Customer 360 - Medallion Architecture Implementation
status: proposed
created: 2026-04-02
updated: 2026-04-02
author: Data Engineering Team
priority: high
tags:
  - medallion-architecture
  - customer-360
  - telco
  - personalization
  - data-platform
---

# Telco Customer 360 - Medallion Architecture Implementation

## Overview

Transform 26 raw CSV datasets from the Telco workspace into a three-layer medallion architecture (Bronze → Silver → Gold) to enable highly personalized customer insights and 360-degree customer analytics.

## Business Context

### Problem Statement
- Raw telco data (customers, subscribers, billing, support) exists in 26 disconnected CSV files
- No unified view of customer behavior, health, or churn risk
- Limited ability to deliver personalized experiences
- Manual effort required for customer analysis

### Business Value
- **Reduce churn** by 15% through proactive retention campaigns
- **Increase ARPU** by 10% via personalized product recommendations
- **Improve NPS** by +5 points through better customer understanding
- **Reduce support costs** by 20% through self-service insights

### Success Metrics
- Pipeline success rate > 99%
- Data freshness < 1 hour
- Query performance P95 < 5 seconds on Gold tables
- Enable 6+ personalization use cases

## Architecture

### Data Sources
**Location:** Telco workspace → telco_data.Lakehouse/Files/telco_data/

**26 CSV Files (1.6 MB total):**

**Core Customer & Party (5 files):**
- party.csv (16.9 KB) - Customer master data
- customer_account.csv (9.4 KB) - Account details
- account_party_role.csv (16.3 KB) - Relationships
- address.csv (15.4 KB) - Address master
- party_address.csv (10.9 KB) - Address links

**Subscriber & Service (6 files):**
- subscriber.csv (19.2 KB)
- subscription.csv (27.2 KB)
- subscriber_status_history.csv (27.3 KB)
- service.csv (24.9 KB)
- service_order.csv (54.1 KB)
- entitlement.csv (34.9 KB)

**Mobile Assets (6 files):**
- msisdn.csv (10.4 KB) - Phone numbers
- subscriber_msisdn.csv (9.6 KB)
- sim.csv (13.0 KB)
- subscriber_sim.csv (9.7 KB)
- device.csv (13.4 KB)
- subscriber_device.csv (10.2 KB)
- porting_request.csv (2.2 KB)

**Billing & Revenue (6 files):**
- invoice.csv (166.6 KB)
- invoice_line.csv (545.2 KB) ⭐ LARGEST
- charge.csv (485.9 KB)
- payment.csv (107.0 KB)
- prepaid_balance_snapshot.csv (58.1 KB)
- topup.csv (27.2 KB)

**Product & Support (2 files):**
- product_catalog.csv (2.4 KB)
- case_ticket.csv (28.2 KB)

### Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                            │
│  Raw Data Preservation (26 Delta Tables)                    │
│  Location: Tables/bronze/{source_file_name}                 │
│  Metadata: _ingestion_ts, _source_file, _row_hash          │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                            │
│  Cleansed & Conformed (7 Domain Tables)                     │
│  Location: Tables/silver/{domain}_{entity}                  │
│                                                              │
│  1. silver_customer_profile                                 │
│  2. silver_subscriber_overview                              │
│  3. silver_mobile_inventory                                 │
│  4. silver_service_catalog                                  │
│  5. silver_billing_transactions                             │
│  6. silver_prepaid_activity                                 │
│  7. silver_support_interactions                             │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                             │
│  Analytics-Ready Business Models (8 Tables)                 │
│  Location: Tables/gold/{use_case}                           │
│                                                              │
│  1. gold_customer_360          - Complete 360° view         │
│  2. gold_subscriber_health     - Engagement metrics         │
│  3. gold_billing_summary       - Revenue analytics          │
│  4. gold_product_affinity      - Cross-sell opportunities   │
│  5. gold_support_analytics     - Service quality            │
│  6. gold_churn_risk_model      - ML-ready churn prediction  │
│  7. gold_customer_journey_events - Timeline/personalization │
│  8. gold_kpi_dashboard         - Executive metrics          │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Plan

### Phase 1: Bronze Layer (Week 1)
**Deliverable:** 26 raw Delta tables with audit columns

**Tasks:**
- [ ] Create Fabric notebook: `01_bronze_ingestion.ipynb`
- [ ] Read all 26 CSV files from Files/telco_data/
- [ ] Write to Delta format in Tables/bronze/
- [ ] Add metadata columns: `_ingestion_ts`, `_source_file`, `_row_hash`
- [ ] Implement data quality checks (row counts, nulls, duplicates)
- [ ] Create bronze data quality dashboard
- [ ] Schedule daily incremental load

**Validation:**
- All 26 bronze tables created
- Row counts match source CSVs
- No data loss during ingestion

### Phase 2: Silver Layer (Weeks 2-3)
**Deliverable:** 7 cleansed domain tables

**Tasks:**
- [ ] Create Fabric notebook: `02_silver_transformations.ipynb`
- [ ] Build silver_customer_profile (join party, account, address)
- [ ] Build silver_subscriber_overview (merge subscriber + status history)
- [ ] Build silver_mobile_inventory (MSISDN, SIM, device associations)
- [ ] Build silver_service_catalog (services + entitlements + orders)
- [ ] Build silver_billing_transactions (invoices + charges + payments)
- [ ] Build silver_prepaid_activity (balances + topups)
- [ ] Build silver_support_interactions (cases + porting requests)
- [ ] Implement SCD Type 2 for historical tracking
- [ ] Add data quality rules (uniqueness, referential integrity)
- [ ] Schedule daily refresh cascading from bronze

**Validation:**
- All 7 silver tables created
- Foreign key relationships validated
- Data quality tests pass (> 95% completeness on critical fields)

### Phase 3: Gold Layer (Weeks 3-4)
**Deliverable:** 8 analytics-ready aggregated tables

**Tasks:**
- [ ] Create Fabric notebook: `03_gold_aggregations.ipynb`
- [ ] Build gold_customer_360 (360° profile + KPIs)
- [ ] Build gold_subscriber_health (engagement + health scores)
- [ ] Build gold_billing_summary (revenue analytics)
- [ ] Build gold_product_affinity (product usage patterns)
- [ ] Build gold_support_analytics (support quality metrics)
- [ ] Build gold_churn_risk_model (churn prediction features)
- [ ] Build gold_customer_journey_events (timeline events)
- [ ] Build gold_kpi_dashboard (executive metrics)
- [ ] Implement composite scoring algorithms
- [ ] Schedule hourly/daily refresh based on SLA

**Validation:**
- All 8 gold tables created
- Business logic verified by analysts
- Query performance < 5 seconds

### Phase 4: Semantic Layer & Reporting (Week 5)
**Deliverable:** Power BI reports and semantic model

**Tasks:**
- [ ] Create Direct Lake semantic model
- [ ] Build Customer 360 Power BI report
- [ ] Build Churn Analytics dashboard
- [ ] Build Revenue & Billing dashboard
- [ ] Build Executive KPI dashboard
- [ ] Publish to Fabric workspace
- [ ] Configure RLS for data security

**Validation:**
- Reports render < 3 seconds
- All KPIs accurate
- User acceptance testing passed

### Phase 5: Operationalization (Week 6)
**Deliverable:** Production-ready data pipeline

**Tasks:**
- [ ] Create Fabric Data Pipeline for orchestration
- [ ] Implement incremental processing logic
- [ ] Set up monitoring & alerting (Spark job failures, DQ)
- [ ] Configure retry logic and error handling
- [ ] Document data lineage in Purview
- [ ] Create business glossary
- [ ] Write user documentation
- [ ] Conduct knowledge transfer

**Validation:**
- Pipeline runs successfully end-to-end
- Monitoring alerts configured
- Documentation complete

## Technical Specifications

### Bronze Layer Schema
```python
bronze_metadata = {
    "_ingestion_ts": "timestamp",      # When data was loaded
    "_source_file": "string",          # Original CSV filename
    "_row_hash": "string",             # MD5 hash for deduplication
    "_is_current": "boolean"           # For incremental loads
}
```

### Silver Layer Transformations

**1. silver_customer_profile**
```sql
SELECT 
    p.party_id as customer_id,
    p.party_type,
    p.display_name,
    p.email,
    p.birth_date,
    DATEDIFF(CURRENT_DATE, p.created_ts) as tenure_days,
    a.city,
    a.postal_code,
    a.province,
    ca.account_status,
    ca.account_type,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, p.created_ts) < 90 THEN 'New'
        WHEN DATEDIFF(CURRENT_DATE, p.created_ts) < 365 THEN 'Growing'
        ELSE 'Established'
    END as lifecycle_stage
FROM bronze_party p
JOIN bronze_account_party_role apr ON p.party_id = apr.party_id
JOIN bronze_customer_account ca ON apr.account_id = ca.account_id
LEFT JOIN bronze_party_address pa ON p.party_id = pa.party_id
LEFT JOIN bronze_address a ON pa.address_id = a.address_id
WHERE apr.role_type = 'Primary'
```

**2. silver_subscriber_overview**
```sql
SELECT
    s.subscriber_id,
    s.customer_id,
    s.subscriber_type,
    sub.subscription_id,
    sub.status as current_status,
    sub.activation_date,
    DATEDIFF(CURRENT_DATE, sub.activation_date) as subscriber_tenure_days,
    ssh.status_change_count,
    ssh.last_status_change_date
FROM bronze_subscriber s
JOIN bronze_subscription sub ON s.subscriber_id = sub.subscriber_id
LEFT JOIN (
    SELECT 
        subscriber_id,
        COUNT(*) as status_change_count,
        MAX(change_date) as last_status_change_date
    FROM bronze_subscriber_status_history
    GROUP BY subscriber_id
) ssh ON s.subscriber_id = ssh.subscriber_id
WHERE sub.is_active = true
```

### Gold Layer Business Logic

**gold_churn_risk_model - Scoring Algorithm**
```python
def calculate_churn_risk_score(subscriber_data):
    """
    Churn risk score: 0 (low) to 1 (high)
    Weighted composite of multiple signals
    """
    score = 0.0
    
    # Payment behavior (30% weight)
    if subscriber_data['overdue_days'] > 30:
        score += 0.30
    elif subscriber_data['overdue_days'] > 0:
        score += 0.15
    
    # Usage trend (25% weight)
    if subscriber_data['usage_trend_3m'] < -0.3:  # 30% decline
        score += 0.25
    elif subscriber_data['usage_trend_3m'] < -0.1:  # 10% decline
        score += 0.15
    
    # Support interactions (20% weight)
    if subscriber_data['support_tickets_90d'] > 5:
        score += 0.20
    elif subscriber_data['support_tickets_90d'] > 2:
        score += 0.10
    
    # Engagement (15% weight)
    if subscriber_data['days_since_last_topup'] > 60:
        score += 0.15
    elif subscriber_data['days_since_last_topup'] > 30:
        score += 0.08
    
    # Contract status (10% weight)
    if subscriber_data['out_of_contract']:
        score += 0.10
    
    return min(score, 1.0)  # Cap at 1.0
```

## Data Quality Rules

### Critical Fields (< 5% null tolerance)
- customer_id, subscriber_id, msisdn
- invoice_id, payment_id
- party.email (for B2C)
- subscription.status

### Uniqueness Constraints
- bronze_party.party_id (PK)
- bronze_subscriber.subscriber_id (PK)
- bronze_invoice.invoice_id (PK)
- bronze_msisdn.msisdn (unique phone number)

### Referential Integrity
- subscriber.customer_id → customer_account.customer_id
- subscription.subscriber_id → subscriber.subscriber_id
- invoice.account_id → customer_account.account_id
- payment.invoice_id → invoice.invoice_id

### Data Freshness SLA
- Bronze: < 2 hours from source update
- Silver: < 4 hours from bronze update
- Gold: < 6 hours from silver update (hourly refresh for key tables)

## Personalization Use Cases Enabled

### 1. Real-Time Customer Service
**Query:** `gold_customer_360` by customer_id
**Trigger:** Agent receives customer call
**Action:** Display 360° view (tenure, payment status, open tickets, churn risk)
**Personalization:** Offer retention discount if churn_risk_score > 0.7

### 2. Proactive Retention Campaigns
**Query:** `gold_churn_risk_model` WHERE churn_risk_tier = 'High'
**Trigger:** Daily batch job at 6 AM
**Action:** Send personalized SMS/email with special offer
**Personalization:** Offer based on contributing_factors (e.g., payment plan if payment_issues)

### 3. Personalized Product Recommendations
**Query:** `gold_product_affinity` WHERE bundle_coverage_gap IS NOT NULL
**Trigger:** Monthly product recommendation engine
**Action:** In-app notification or next invoice insert
**Personalization:** Family plan if multiple subscribers, international if roaming usage detected

### 4. Payment Default Prevention
**Query:** `gold_billing_summary` WHERE payment_on_time_flag = false
**Trigger:** 3 days before due date
**Action:** Auto-reminder via SMS
**Personalization:** Flexible payment plan offer for customers with good history

### 5. Device Upgrade Campaigns
**Query:** `gold_subscriber_health` WHERE device_upgrade_eligible = true
**Trigger:** Quarterly campaign
**Action:** Personalized email with device recommendations
**Personalization:** Trade-in value calculated, device suggestions based on current device tier

### 6. Customer Journey Optimization
**Query:** `gold_customer_journey_events` aggregated by event_category
**Trigger:** Weekly analytics review
**Action:** Identify drop-off points and friction
**Personalization:** A/B test interventions at critical journey stages

## Risks & Mitigations

### Risk 1: Data Quality Issues in Source CSVs
**Impact:** High - Garbage in, garbage out
**Mitigation:** 
- Implement strict validation in Bronze layer
- Create data quality dashboard for monitoring
- Alert on anomalies (row count drops, null spikes)

### Risk 2: Scalability Beyond Initial Dataset
**Impact:** Medium - Performance degradation as data grows
**Mitigation:**
- Design for incremental processing from start
- Implement partitioning strategy (by date, customer_id)
- Use Delta Lake Z-ORDER for query optimization

### Risk 3: Business Logic Complexity
**Impact:** Medium - Incorrect insights lead to poor decisions
**Mitigation:**
- Extensive validation with business analysts
- Unit tests for scoring algorithms
- Shadow run period before production

### Risk 4: Privacy/Compliance
**Impact:** High - GDPR violations
**Mitigation:**
- PII masking in non-production environments
- Implement RLS in semantic layer
- Data retention policies (Bronze: 2yr, Silver: 5yr, Gold: 7yr)
- Document data lineage in Purview

## Dependencies

### Platform Requirements
- Microsoft Fabric workspace with Premium capacity
- Lakehouse: telco_data (already exists)
- Spark compute for notebooks
- Power BI Premium for Direct Lake

### Skillset Requirements
- Data Engineer (PySpark, Delta Lake, Fabric)
- Analytics Engineer (SQL, data modeling)
- Business Analyst (Telco domain knowledge)
- ML Engineer (churn prediction model - optional Phase 2)

### External Dependencies
- Source CSV files available in Files/telco_data/
- No dependencies on external APIs or systems

## Testing Strategy

### Unit Tests
- Bronze ingestion: row count validation
- Silver transformations: join logic correctness
- Gold calculations: scoring algorithm accuracy

### Integration Tests
- End-to-end pipeline: Bronze → Silver → Gold
- Data lineage validation
- Performance benchmarks

### User Acceptance Testing
- Business analysts validate gold table content
- Power BI reports reviewed by stakeholders
- Personalization use cases tested in pilot

## Rollout Plan

### Phase 1: Development (Weeks 1-4)
- Build Bronze, Silver, Gold layers in dev environment
- Unit and integration testing

### Phase 2: UAT (Week 5)
- Deploy to UAT environment
- Business validation
- Performance tuning

### Phase 3: Pilot (Week 6)
- Deploy to production
- Shadow run (no live actions)
- Monitor for 1 week

### Phase 4: Production (Week 7+)
- Enable live personalization use cases
- Monitor KPIs
- Iterate based on feedback

## Cost Estimate

### Development (6 weeks)
- 1 FTE Data Engineer @ $150k/year = ~$17k
- 0.5 FTE Analytics Engineer @ $120k/year = ~$7k
- **Total Dev Cost: $24k**

### Ongoing Operational Costs (monthly)
- Fabric Compute (Spark): ~$500
- Storage (Delta Lake): ~$100
- Power BI Premium: ~$500
- **Total Monthly: $1,100**

### Expected ROI
- Churn reduction (15% × $50 ARPU × 100 customers): $750/month
- ARPU increase (10% × $50 × 1000 customers): $5,000/month
- **Total Monthly Benefit: $5,750**
- **Payback Period: ~5 months**

## References

- [Telco Medallion Architecture Proposal](../telco-medallion-architecture.md)
- [Source CSV Files](../output/)
- [Telco Schema Documentation](../Telco_customer_schema.txt)
- [Sample Notebook](../telco_tables_creation.ipynb)

## Approval & Sign-off

- [ ] Data Engineering Lead
- [ ] Analytics Manager
- [ ] Business Owner (Telco Product)
- [ ] Architecture Review Board
- [ ] Security & Compliance

---

**Next Steps:**
1. Review and approve this specification
2. Allocate resources (data engineer + analyst)
3. Set up Fabric environment
4. Begin Phase 1: Bronze Layer implementation
