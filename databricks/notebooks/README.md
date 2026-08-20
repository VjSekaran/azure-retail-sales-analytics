# Databricks Notebooks

This folder contains the PySpark and Spark SQL implementation used for the Azure Retail Sales Analytics project.

## Notebook Workflow

The notebook follows the project data-engineering flow:

1. Read processed data from ADLS Gen2
2. Create Bronze Delta tables
3. Clean and standardize data in Silver
4. Create Gold dimension tables
5. Create the Gold fact table
6. Perform data-quality checks
7. Validate the final analytical dataset

## Main Transformations

- Null handling
- Duplicate removal
- String standardization
- Data type conversion
- Business-rule validation
- Dimension creation
- Fact-table creation
- Referential-integrity checks
- Final reconciliation

## Technologies

- PySpark
- Spark SQL
- Delta Lake
- Azure Databricks
- ADLS Gen2
