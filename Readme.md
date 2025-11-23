# AWS RDS to BigQuery Data Pipeline

## Description
A serverless ELT (Extract, Load, Transform) data pipeline designed to move transactional data from AWS RDS to Google BigQuery for analytics and reporting purposes. The pipeline leverages AWS Lambda for cost-effective data extraction and ingestion, and utilizes the Medallion Architecture (Bronze, Silver, Gold layers) within BigQuery to organize, clean, and model the data for downstream consumption by BI tools like Power BI or Looker.

## Key Components
- **Source System**: AWS RDS (MySQL)
- **Ingestion Layer**: AWS Lambda (Python) triggered by Amazon EventBridge Scheduler.
- **Storage (Data Warehouse)**: Google BigQuery
- **Transformation**: BigQuery Scheduled Queries (SQL).
- **Orchestration**: GCP Workflows
