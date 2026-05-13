# Catastrophe Risk Intelligence Pipeline

A real-time streaming data pipeline built on Google Cloud Platform to process catastrophe events from public APIs.

---

## Overview

This project demonstrates building a production-grade streaming architecture on GCP, processing real-time catastrophe data from NOAA, USGS, and FEMA APIs. The pipeline calculates risk scores, aggregates events in 15-minute windows, and surfaces insights through a live dashboard.

---

## Architecture

Public APIs (NOAA, USGS, FEMA)
↓
Cloud Functions → Cloud Scheduler
↓
Cloud Pub/Sub
↓
Cloud Dataflow (Apache Beam)
↓
BigQuery (partitioned & clustered)
↓
Looker Studio Dashboard
Orchestrated by: Cloud Composer (Airflow)

---

## Tech Stack

**Data Pipeline:**
- Cloud Functions (Python) — API ingestion every 15 minutes
- Cloud Pub/Sub — Message streaming buffer
- Cloud Dataflow + Apache Beam — Stream processing with windowing
- BigQuery — Partitioned by day, clustered by source/event_type/severity

**Orchestration & Monitoring:**
- Cloud Composer (Apache Airflow) — DAG scheduling, retries, alerts
- Cloud Scheduler — Trigger ingestion functions

**Infrastructure:**
- Terraform — Infrastructure as Code (18 resources)
- GitHub Actions — CI/CD (automated testing + deployment)

**Visualization:**
- Looker Studio — Live dashboard with 15-min auto-refresh

---

## Key Features

### Real-Time Stream Processing
- Apache Beam windowing (15-minute fixed windows)
- Event time processing with late data handling
- Risk score calculation based on magnitude, severity, and event type

### Production Reliability
- Automatic retries on failure via Cloud Composer
- Pipeline health monitoring (events per 15-min window)
- Data validation at every stage

### Cost Optimization
- BigQuery partitioning (daily) reduces query costs
- Clustering (source, event_type, severity) enables partition pruning
- Serverless auto-scaling with Dataflow

### Infrastructure as Code
- Complete stack defined in Terraform
- One-command deployment: `terraform apply`
- Automated teardown: `terraform destroy`

---

## Data Sources

All data is from public APIs — no proprietary data used:

| Source | Data Type |
|---|---|
| **NOAA** | Weather alerts, storms, hurricanes |
| **USGS** | Real-time earthquake events (M2.5+) |
| **FEMA** | US disaster declarations |

---

## Setup & Deployment

**Prerequisites:**
- GCP account with billing enabled
- Terraform installed locally
- gcloud CLI authenticated

**Deploy:**
```bash
cd terraform/
terraform init
terraform apply
```

**Verify Pipeline:**
```bash
# Check Dataflow job status
gcloud dataflow jobs list

# Query BigQuery
bq query --use_legacy_sql=false \
'SELECT COUNT(*) FROM `cat-risk-pipeline-2026.cat_risk.events`'
```

**Teardown:**
```bash
terraform destroy
```

---

## Testing

Run unit tests:
```bash
pytest tests/ -v
```

Tests cover:
- API connectivity (USGS, NOAA, FEMA)
- Pub/Sub publish/subscribe
- Apache Beam transform logic
- Data validation and enrichment

---

## Sample Queries

**Top 10 high-risk regions today:**
```sql
SELECT 
  location, 
  COUNT(*) as event_count,
  ROUND(AVG(risk_score), 2) as avg_risk_score
FROM `cat-risk-pipeline-2026.cat_risk.events`
WHERE DATE(timestamp) = CURRENT_DATE()
GROUP BY location
ORDER BY avg_risk_score DESC
LIMIT 10;
```

**Event volume trends (last 7 days):**
```sql
SELECT 
  DATE(timestamp) as event_date,
  event_type,
  COUNT(*) as count
FROM `cat-risk-pipeline-2026.cat_risk.events`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY event_date, event_type
ORDER BY event_date DESC;
```

More queries in `bigquery/queries/`

