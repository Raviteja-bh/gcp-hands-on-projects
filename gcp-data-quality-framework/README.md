# Serverless Data Quality Firewall (GCP & Great Expectations)

An event-driven, serverless data validation pipeline built on Google Cloud Platform (GCP) using the Great Expectations (GX) Fluent API. 
This framework intercepts incoming production datasets, programmatically audits them against strict schema and business logic, 
and executes automated row-level quarantine routing to prevent downstream database corruption.

---

## Architecture Overview

```mermaid
graph TD
    A[Cloud Shell / CLI Upload] -->|gcloud storage cp| B(GCS Landing Bucket)
    B -->|Event Trigger| C[GCP Cloud Function Gen 2]
    subgraph Execution Layer [Cloud Function + Great Expectations]
        C --> D{GX Validation Gate}
    end
    D -->|Passed Rows| E[Google BigQuery]
    D -->|Failed Rows| F(GCS Quarantine Bucket)
    D -->|Generate Report| G(GCS Data Docs Bucket)

    style C fill:#4285F4,stroke:#333,stroke-width:2px,color:#fff
    style E fill:#0F9D58,stroke:#333,stroke-width:2px,color:#fff
    style F fill:#DB4437,stroke:#333,stroke-width:2px,color:#fff
    style G fill:#F4B400,stroke:#333,stroke-width:2px,color:#fff
