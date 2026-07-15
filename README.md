# Financial DataOps Pipeline on AWS

## Project Overview

This project demonstrates an end-to-end DataOps pipeline for financial transaction processing on AWS. The solution ingests raw financial data from Amazon S3, performs data cleansing and transformation using AWS Glue (PySpark), builds curated analytical datasets using AWS Glue Studio Visual ETL, and stores the results as partitioned Parquet files for downstream analytics.

The project was developed as part of a Senior DataOps Engineer technical assessment and demonstrates Infrastructure as Code (Terraform), CI/CD (GitHub Actions), orchestration (AWS Step Functions), monitoring (CloudWatch), and data quality best practices.

---

# Architecture

Bronze (Raw Data)
        ↓
AWS Glue PySpark
        ↓
Silver (Cleaned Data)
        ↓
AWS Glue Studio Visual ETL
        ↓
Gold (Analytics Ready)
        ↓
Glue Data Catalog
        ↓
Amazon Athena

Pipeline orchestration is performed using AWS Step Functions.

Infrastructure is provisioned using Terraform.

Continuous Integration is implemented using GitHub Actions.

---

# Dataset

Source

Financial Transactions Dataset: Analytics

https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets

Files used

- Transactions
- Fraud Labels
- Customer Profiles
- Card Information
- Merchant Category Codes (MCC)

Dataset Size

~1.5 GB

---

# Technology Stack

| Service | Purpose |
|----------|----------|
| Amazon S3 | Bronze, Silver and Gold Data Lake |
| AWS Glue | PySpark ETL |
| AWS Glue Studio | Visual ETL |
| AWS Glue Crawlers | Metadata Discovery |
| AWS Glue Data Catalog | Metadata Repository |
| Amazon Athena | SQL Analytics |
| AWS Step Functions | Pipeline Orchestration |
| Amazon CloudWatch | Monitoring and Logging |
| Terraform | Infrastructure as Code |
| GitHub Actions | Continuous Integration |
| IAM | Security |

---

# Data Lake Architecture

## Bronze Layer

Stores immutable raw files exactly as received.

Examples

- transactions.csv
- fraud_labels.json
- cards.csv
- users.csv
- mcc.json

---

## Silver Layer

AWS Glue PySpark performs

- Data cleansing
- Schema validation
- Null handling
- Standardised timestamps
- Fraud label normalisation
- Removal of duplicate records
- Sensitive data masking
- Parquet conversion

Outputs

- silver_transactions
- silver_cards
- silver_users
- silver_mcc
- silver_fraud_labels

---

## Gold Layer

AWS Glue Studio Visual ETL performs

- Business joins
- Fraud enrichment
- Merchant enrichment
- Card enrichment
- Analytics-ready dataset generation

Output

gold_fraud_analytics

Stored as partitioned Parquet in Amazon S3.

---

# Infrastructure as Code

Infrastructure is fully managed using Terraform.

Resources provisioned

- S3 Buckets
- IAM Roles
- IAM Policies
- Glue Database
- Glue Jobs
- Glue Crawlers
- CloudWatch Log Groups
- Step Functions

---

# CI/CD

## Continuous Integration

GitHub Actions automatically performs

- Python dependency installation
- Ruff linting
- Pytest execution
- Terraform formatting validation
- Terraform configuration validation

## Continuous Deployment

Deployment follows a controlled release approach.

Updated Glue scripts are deployed through Terraform and executed through AWS Step Functions.

---

# Pipeline Orchestration

AWS Step Functions orchestrates the pipeline.

Workflow

Raw to Silver Glue Job

↓

Silver to Gold Visual ETL Job

↓

Gold Glue Crawler

↓

Pipeline Success

The workflow automatically waits for each Glue job to complete before starting the next stage.

---

# Data Quality

Implemented quality checks include

- Mandatory field validation
- Duplicate detection
- Data type standardisation
- Null handling
- Fraud label validation
- Transaction schema validation

---

# Security

Security measures include

- IAM Least Privilege
- Environment variables for configuration
- No AWS credentials stored in Git
- Terraform-managed IAM permissions
- Encrypted S3 storage

---

# Monitoring

Monitoring is implemented using

- AWS CloudWatch Logs
- Glue Job Run History
- Step Functions Execution History
- GitHub Actions Workflow History

---

# Repository Structure

```text
financial-dataops-aws/

├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
│
├── architecture/
│
├── glue/
│
├── scripts/
│
├── src/
│
├── terraform/
│
├── tests/
│
├── build/
│
├── README.md
│
├── requirements.txt
│
└── .gitignore
```

# Running the Pipeline

## 1 Deploy Infrastructure

```bash
terraform init
terraform plan
terraform apply
```

## 2 Upload Bronze Data

Upload the Kaggle dataset into the Bronze S3 bucket.

## 3 Execute Pipeline

Run the AWS Step Functions state machine.

The workflow executes

- Raw to Silver Glue Job
- Silver to Gold Visual ETL Job
- Gold Crawler

## 4 Query Gold Layer

Use Amazon Athena to query

```
gold_fraud_analytics
```

Example

```sql
SELECT
is_fraud,
COUNT(*)
FROM gold_fraud_analytics
GROUP BY is_fraud;
```

# Future Improvements

Potential production enhancements

- Amazon Redshift Serverless
- Amazon SageMaker Fraud Detection
- Great Expectations
- Multi-environment deployments
- Automated Terraform deployment
- Data Quality Dashboards

# Project Outcome

The solution demonstrates an end-to-end DataOps implementation using AWS managed services.

Key capabilities include

- Infrastructure as Code
- Automated ETL
- Visual ETL
- Data Lake Architecture
- CI/CD
- Pipeline Orchestration
- Monitoring
- Data Quality
- Security
- Analytics-ready Gold datasets