# 🚀 Azure Retail Sales Analytics

> **An end-to-end retail data engineering and business intelligence project using Azure Data Factory, ADLS Gen2, Azure Databricks, PySpark, Delta Lake, Star Schema and Power BI.**

---

## 📌 Project Overview

This project demonstrates an end-to-end retail analytics solution that transforms raw retail data into analytics-ready datasets and interactive Power BI dashboards.

The solution covers the complete data journey:

**Data Ingestion → Data Storage → Data Transformation → Data Quality → Dimensional Modeling → Power BI Analytics**

The project was designed to demonstrate practical experience across both **Azure Data Engineering** and **Business Intelligence**.

### Key Areas Covered

- Cloud data ingestion
- Azure Data Factory orchestration
- ADLS Gen2 storage
- Databricks data processing
- PySpark transformations
- Spark SQL validation
- Delta Lake
- Medallion Architecture
- Data quality and validation
- Star Schema
- Power BI semantic modeling
- DAX measures
- Interactive dashboards

---

# 🎯 Business Problem

Retail businesses need reliable and centralized data to understand their sales performance across different business dimensions.

The objective of this project was to create an analytical solution that can help answer questions such as:

- What is the overall sales performance?
- Which regions generate the highest sales?
- Which products and categories contribute the most revenue?
- Who are the top customers?
- Which salespeople perform best?
- Which territories generate the most sales?
- How does sales performance change over time?
- Are the analytical results reliable and validated?

The solution therefore focuses not only on visualization, but also on the complete data engineering process behind the dashboards.

---

# 🏗️ Solution Architecture

The project follows a layered Azure data architecture.

```text
Source CSV Files
       ↓
Azure Data Factory
       ↓
ADLS Gen2 – Raw / Processed Data
       ↓
Azure Databricks
       ↓
Bronze Layer
       ↓
Silver Layer
       ↓
Gold Layer
       ↓
Star Schema
       ↓
Power BI Semantic Model
       ↓
Interactive Dashboards

Architecture Diagram

Architecture Components
Component	Purpose
CSV Files	Source retail data
Azure Data Factory	Data ingestion and orchestration
ADLS Gen2	Cloud storage
Azure Databricks	Data processing and transformation
PySpark	Data transformation
Spark SQL	Data validation and analysis
Delta Lake	Reliable analytical storage
Bronze Layer	Source-aligned data
Silver Layer	Cleaned and standardized data
Gold Layer	Analytics-ready data
Star Schema	Dimensional analytical model
Power BI	Business intelligence and visualization
🛠️ Technology Stack
Cloud
Microsoft Azure
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
Data Engineering
Apache Spark
PySpark
Spark SQL
Delta Lake
Medallion Architecture
Data Modeling
Star Schema
Fact Tables
Dimension Tables
Semantic Modeling
Business Intelligence
Power BI
DAX
Interactive Dashboards
🔄 Data Ingestion with Azure Data Factory

Azure Data Factory is used as the ingestion and orchestration layer of the solution.

The project includes a pipeline that uses metadata-driven processing to identify and process source files.

Pipeline Flow
Get Metadata
      ↓
ForEach Raw File
      ↓
Identify Source File
      ↓
Data Flow / Copy Processing
      ↓
Processed Data in ADLS Gen2
      ↓
Databricks Transformation
ADF Pipeline

The repository contains the ADF pipeline implementation:

adf/pl_copy_orders_raw_to_processed.json

Why Azure Data Factory?

Azure Data Factory provides:

Data ingestion
Pipeline orchestration
Parameterization
Dynamic file processing
Integration with Azure storage
Repeatable and maintainable workflows
🗄️ ADLS Gen2

Azure Data Lake Storage Gen2 acts as the cloud storage layer.

The data is organized so that the ingestion and processing stages can be separated from the analytical layer.

Benefits
Scalable cloud storage
Centralized data repository
Integration with Azure Data Factory
Integration with Azure Databricks
Support for analytical workloads
🧱 Medallion Architecture

The Databricks processing layer follows the Medallion Architecture.

Raw / Processed Data
        ↓
     🥉 Bronze
        ↓
     🥈 Silver
        ↓
     🥇 Gold
        ↓
 Power BI Semantic Model
🥉 Bronze Layer

The Bronze layer stores the ingested data in Delta format while maintaining the structure of the incoming datasets.

Main Objective
Preserve ingested data
Create Delta tables
Maintain a reliable source for downstream transformations
Provide traceability for data-quality investigations
🥈 Silver Layer

The Silver layer contains cleaned and standardized data.

Transformations Performed
Null filtering
Duplicate removal
String trimming
Data-type validation
Region standardization
Category standardization
Business-rule validation
Invalid-record handling

For example, order records are filtered for required fields, duplicates are removed, and important string fields are trimmed before being written to the Silver Delta tables.

🥇 Gold Layer

The Gold layer contains analytics-ready datasets designed for business reporting.

The Gold layer follows a Star Schema with a central sales fact table and supporting dimensions.

Fact Table
fact_sales
Dimension Tables
dim_customer
dim_product
dim_salesperson
dim_date
⭐ Gold Layer Star Schema

The central fact_sales table contains sales transactions and connects to the relevant dimension tables.

                 dim_customer
                      │
                      │
                      ▼
dim_product ──── fact_sales ──── dim_salesperson
                      │
                      │
                      ▼
                  dim_date
Dimension Relationships

The Power BI model uses one-to-many relationships from the dimension tables to the central fact table.

This allows users to analyze sales from multiple perspectives such as:

Customer
Product
Date
Salesperson
Region
Category
Segment
Territory
🔄 End-to-End Project Flow

The complete project workflow is:

1. Source CSV Data
        ↓
2. Azure Data Factory Ingestion
        ↓
3. ADLS Gen2
        ↓
4. Databricks Bronze Layer
        ↓
5. Databricks Silver Layer
        ↓
6. Databricks Gold Layer
        ↓
7. Star Schema
        ↓
8. Power BI Semantic Model
        ↓
9. Business Dashboards
🧹 Data Quality & Validation

Data quality is an important part of this project.

Instead of directly consuming the source data, multiple validation and transformation checks were performed before the data reached the Gold layer.

Data Quality Checks

The project includes checks for:

Null values
Duplicate records
Invalid quantity values
Sales and discount ranges
Region consistency
Product category consistency
Unmatched products
Unmatched customers
Unmatched salespersons
Fact-to-dimension relationships
Final Gold-layer reconciliation
🔎 Zero-Quantity Investigation

During validation, a zero-quantity sales record was identified and investigated.

The original Bronze record was traced before applying the data-quality rule.

The Silver transformation subsequently filters order items so that only records with:

Quantity > 0

are retained.

This demonstrates an important data engineering principle:

Do not blindly remove suspicious records. Investigate the source, understand the business rule, and then apply the appropriate treatment.

👤 Missing Salesperson Handling

The project also includes handling for missing salesperson assignments.

Missing salesperson IDs are mapped to:

UNASSIGNED

An UNASSIGNED record is then added to the salesperson dimension so that the fact-to-dimension relationship remains valid.

This allows the dashboard to retain those sales records without breaking dimensional integrity.

🔍 Referential Integrity

The Gold layer includes validation checks for unmatched dimension records.

The following relationships are validated:

fact_sales → dim_product
fact_sales → dim_customer
fact_sales → dim_salesperson

The notebook also rechecks the salesperson relationship after applying the UNASSIGNED treatment.

📊 Power BI Analytics

The Gold-layer data is connected to Power BI and modeled using a Star Schema.

The report contains three main analytical pages.

1️⃣ Sales Overview

The Sales Overview dashboard provides a high-level view of overall sales performance.

KPIs
Total Sales
Average Order Value
Total Orders
Total Quantity
Visual Analysis
Sales by Region
Monthly Sales Trends
Sales by Category
Sales by Channel

2️⃣ Product & Customer Analysis

This dashboard focuses on product and customer performance.

Analysis Includes
Sales by Product
Sales by Subcategory
Top 10 Customers
Sales by Category
Sales by Customer Segment
Region × Category Analysis

3️⃣ Salesperson Performance

This dashboard focuses on salesperson and territory performance.

Analysis Includes
Top Salespeople by Sales
Sales by Territory
Sales by Manager
Salesperson Performance Matrix
Total Orders
Total Sales
Average Order Value
Sales Contribution %

📐 Power BI Semantic Model

The Power BI semantic model contains:

Fact Table

fact_sales

Dimension Tables

dim_customer

dim_product

dim_salesperson

dim_date

The model uses a Star Schema where the dimension tables filter the central fact table.

This structure helps provide:

Consistent filtering
Better analytical performance
Reusable business logic
Easier DAX development
Clear business relationships
📊 Core Power BI Measures

The report uses measures for key business KPIs.

Total Sales
Total Sales =
SUM(fact_sales[SalesAmount])
Total Orders
Total Orders =
DISTINCTCOUNT(fact_sales[OrderID])
Total Quantity
Total Quantity =
SUM(fact_sales[Quantity])
Average Order Value
Average Order Value =
DIVIDE(
    [Total Sales],
    [Total Orders]
)

These measures are used throughout the Power BI report to provide consistent KPI calculations.

🎛️ Interactive Analysis

The dashboards provide interactive filtering across important business dimensions.

Filters
Calendar / Date
Order Channel
Region
Category

This allows users to drill into specific combinations of:

Time
+
Channel
+
Region
+
Category

and understand how those selections affect the business metrics and visuals.

📈 Business Insights Enabled

The solution enables analysis across several business areas.

Sales Performance
Overall revenue
Regional sales
Monthly trends
Channel contribution
Product Performance
Product-level sales
Category performance
Subcategory performance
Customer Analysis
Top customers
Customer segments
Average order value
Salesperson Performance
Top salespeople
Territory performance
Manager contribution
Sales contribution
🔍 Final Validation

Before Power BI consumption, the Gold fact table is validated using Spark SQL.

The validation includes:

Total line items
Distinct orders
Total quantity
Total sales
First order date
Last order date

Example validation query:

SELECT
    COUNT(*) AS Total_Line_Items,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales,
    MIN(OrderDate) AS First_Order_Date,
    MAX(OrderDate) AS Last_Order_Date
FROM gold.fact_sales;

Additional validation is performed for:

Sales by region
Sales by product category
Order channel
Sales and discount ranges
Zero-quantity records
Unmatched products
Unmatched customers
Unmatched salespersons
📚 Project Documentation

Detailed learning material and project documentation are available in:

documentation/

The documentation covers:

Azure environment setup
ADLS Gen2
Azure Data Factory
Databricks
PySpark
Spark
Delta Lake
Bronze / Silver / Gold architecture
Data quality
Star Schema
Power BI
DAX
Dashboard development
Project flow
Interview preparation
📂 Repository Structure
azure-retail-sales-analytics/
│
├── README.md
│
├── architecture/
│   ├── README.md
│   ├── end-to-end-architecture.png
│   ├── end-to-end-project-flow.png
│   └── gold-star-schema.png
│
├── adf/
│   ├── README.md
│   ├── pipeline-overview.png
│   └── pl_copy_orders_raw_to_processed.json
│
├── databricks/
│   ├── README.md
│   └── notebooks/
│       ├── README.md
│       └── 01_retail_bronze_silver_gold.py
│
├── powerbi/
│   ├── README.md
│   ├── sales-overview.png
│   ├── product-customer-analysis.png
│   └── salesperson-performance.png
│
└── documentation/
    ├── README.md
    └── Azure Retail Analytics Study Guide.docx
💡 Key Learnings

This project provided hands-on experience in:

Azure Data Engineering
Azure Data Factory
ADLS Gen2
Azure Databricks
Data ingestion
Pipeline orchestration
Data Engineering
PySpark
Spark SQL
Delta Lake
Medallion Architecture
Data cleaning
Data validation
Data Modeling
Fact tables
Dimension tables
Star Schema
One-to-many relationships
Power BI semantic modeling
Business Intelligence
Power BI
DAX
KPI development
Interactive filtering
Dashboard design
Business analysis
🎯 Project Outcome

The project demonstrates the complete journey from raw retail data to business intelligence:

Raw Data
   ↓
Azure Data Factory
   ↓
ADLS Gen2
   ↓
Databricks
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Star Schema
   ↓
Power BI Semantic Model
   ↓
Interactive Dashboards
   ↓
Business Insights

The final solution combines Azure Data Engineering and Power BI Analytics into an end-to-end retail sales analytics platform.

🚀 What This Project Demonstrates

This project demonstrates practical ability to:

Build an end-to-end Azure data pipeline
Ingest and process cloud data
Transform data using PySpark
Implement Bronze, Silver and Gold layers
Perform data-quality investigations
Build analytics-ready dimensional models
Create Power BI semantic models
Develop DAX measures
Design interactive business dashboards
Validate analytical results before reporting
👨‍💻 Author
Vijayasekaran K

Data Analyst | Power BI | SQL | Azure | Databricks | PySpark

⭐ If you found this project useful, feel free to explore the repository and implementation details.



### One important thing


I intentionally **removed the hard-coded validation result table** from the earlier version. Your notebook contains the validation queries, but the file content we have does not show the actual final output values, so it would be better not to claim exact numbers in the README unless you've independently verified them. The notebook clearly shows the final validation logic and the referential-integrity checks. :contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}


I also kept the README aligned with the actual transformations: trimming/deduplication, required-field filtering, category/region standardization, zero-quantity handling, and `UNASSIGNED` salesperson handling. :contentReference[oaicite:6]{index=6} :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8}


**Paste the whole block from `# 🚀 Azure Retail Sales Analytics` to the final ⭐ line.** Then commit it with:


```text
Create professional project README
