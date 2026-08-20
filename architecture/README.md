# Architecture

This folder contains the architecture diagrams for the Azure Retail Sales Analytics project.

## End-to-End Architecture

Source CSV files → Azure Data Factory → ADLS Gen2 → Azure Databricks Bronze/Silver/Gold → Power BI

## Project Flow

The project follows a layered data engineering and analytics workflow:

1. CSV Sources
2. Azure Data Factory
3. ADLS Gen2 Raw Layer
4. Databricks Bronze Layer
5. Databricks Silver Layer
6. Databricks Gold Layer
7. Power BI Semantic Model
8. Business Dashboards

## Data Model

The Gold layer follows a Star Schema with:

- fact_sales
- dim_customer
- dim_product
- dim_salesperson
- dim_date
