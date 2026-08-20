# Azure Databricks

Azure Databricks is used as the transformation, data-quality and analytical processing layer in this project.

## Processing Architecture

The Databricks workflow follows the Medallion Architecture:

Raw / Processed ADLS Data
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Gold Layer
        ↓
Power BI

## Bronze Layer

The Bronze layer stores the ingested data in Delta format while maintaining the structure and traceability of the source data.

## Silver Layer

The Silver layer applies data-cleaning and standardization rules, including:

- Null handling
- Duplicate removal
- String trimming
- Region standardization
- Category standardization
- Data type validation
- Business-rule validation

## Gold Layer

The Gold layer contains analytics-ready tables following a Star Schema:

### Fact

- fact_sales

### Dimensions

- dim_customer
- dim_product
- dim_salesperson
- dim_date

## Data Quality

The project includes validation for:

- Zero-quantity sales
- Missing salesperson assignments
- Duplicate records
- Referential integrity
- Fact and dimension consistency

## Technologies

- Azure Databricks
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Medallion Architecture
