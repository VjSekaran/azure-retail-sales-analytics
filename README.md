# 🚀 Azure Retail Sales Analytics

> **End-to-end retail data engineering and business intelligence project using Azure Data Factory, ADLS Gen2, Azure Databricks, PySpark, Delta Lake and Power BI.**

---

## 📌 Project Overview

This project demonstrates an end-to-end retail analytics solution, starting from raw CSV files and ending with interactive Power BI dashboards.

The solution was designed using a layered data engineering architecture to ingest, transform, validate and model retail sales data before exposing it to business users through Power BI.

### End-to-End Flow

```text
Source CSV Files
       ↓
Azure Data Factory
       ↓
ADLS Gen2 — Raw Layer
       ↓
Databricks — Bronze Layer
       ↓
Databricks — Silver Layer
       ↓
Databricks — Gold Layer
       ↓
Star Schema
       ↓
Power BI Semantic Model
       ↓
Business Dashboards
