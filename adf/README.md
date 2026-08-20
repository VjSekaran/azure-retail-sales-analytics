# Azure Data Factory

Azure Data Factory (ADF) is used as the orchestration and ingestion layer in this project.

## Pipeline

The main pipeline processes multiple retail source files dynamically.

### Pipeline Flow

Get Metadata
↓
ForEach Raw File
↓
Identify Source File
↓
Execute Mapping Data Flow
↓
Processed Data in ADLS Gen2

## Source Files

The pipeline handles the following files:

- orders.csv
- customers.csv
- products.csv
- salespersons.csv
- order_items.csv

## Key ADF Components

- Get Metadata Activity
- ForEach Activity
- If Condition
- Mapping Data Flow
- Parameterized datasets
- Azure Data Lake Storage Gen2

## Objective

The pipeline automates the ingestion and processing of raw retail data before it is consumed by Azure Databricks for further transformation.
