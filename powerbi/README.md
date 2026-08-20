# Power BI Analytics & Dashboards

Power BI is used as the business intelligence and visualization layer of this project.

The Gold-layer data from Azure Databricks is connected to Power BI and organized into a Star Schema to support interactive reporting and business analysis.

## Semantic Model

The Power BI model contains:

### Fact Table

- `fact_sales`

### Dimension Tables

- `dim_customer`
- `dim_product`
- `dim_salesperson`
- `dim_date`

The dimension tables have one-to-many relationships with the central `fact_sales` table.

## Key Measures

The dashboard uses measures to calculate important business KPIs such as:

- Total Sales
- Total Orders
- Total Quantity
- Average Order Value
- Sales Contribution %

## Dashboard Pages

### 1. Sales Overview

Provides a high-level view of overall sales performance.

Key visuals include:

- Total Sales
- Average Order Value
- Total Orders
- Total Quantity
- Sales by Region
- Monthly Sales Trends
- Sales by Category
- Sales by Channel

### 2. Product & Customer Analysis

Provides detailed analysis of product and customer performance.

Key visuals include:

- Sales by Product
- Sales by Subcategory
- Top 10 Customers
- Sales by Category
- Sales by Customer Segment
- Region × Category Analysis

### 3. Salesperson Performance

Provides insights into salesperson and territory performance.

Key visuals include:

- Top Salespeople by Sales
- Sales by Territory
- Sales by Manager
- Salesperson Performance Matrix
- Total Orders
- Total Sales
- Average Order Value
- Sales Contribution %

## Interactive Filters

The dashboards include filters for:

- Date
- Order Channel
- Region
- Category

These filters allow users to interactively analyze sales performance across different business dimensions.

## Dashboard Design

The dashboards were designed with a consistent visual theme and layout to make business insights easy to understand.

The report focuses on:

- Clear KPI presentation
- Consistent filtering
- Comparative analysis
- Trend analysis
- Product and customer segmentation
- Salesperson performance

## Dashboard Screenshots

The `screenshots` folder contains previews of the three Power BI dashboard pages.
