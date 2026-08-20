# Databricks notebook source
print("Retail Databricks environment is ready")

# COMMAND ----------

spark.version

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@<storage-account>.dfs.core.windows.net/retail/"))

# COMMAND ----------

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://processed@<storage-account>.dfs.core.windows.net/retail/orders.csv")

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.bronze.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_analytics.bronze.orders LIMIT 10;

# COMMAND ----------

customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://processed@<storage-account>.dfs.core.windows.net/retail/customers.csv")

display(customers_df)

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.bronze.customers")

# COMMAND ----------

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://processed@<storage-account>.dfs.core.windows.net/retail/products.csv")

display(products_df)

# COMMAND ----------

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.bronze.products")

# COMMAND ----------

salespersons_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://processed@<storage-account>.dfs.core.windows.net/retail/salespersons.csv")

display(salespersons_df)

# COMMAND ----------

salespersons_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.bronze.salespersons")

# COMMAND ----------

# Loading Order Items from ADLS

order_items_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://processed@<storage-account>.dfs.core.windows.net/retail/order_items.csv")

display(order_items_df)

# COMMAND ----------

# Saving Order Items to Bronze Delta Table

order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.bronze.order_items")

# COMMAND ----------

# Listing Tables in Bronze Schema

spark.sql("SHOW TABLES IN retail_analytics.bronze").show()

# COMMAND ----------

# Loading Orders from Bronze Delta Table

orders_bronze_df = spark.table("retail_analytics.bronze.orders")

display(orders_bronze_df)

# COMMAND ----------

# Checking Orders Data Types

orders_bronze_df.printSchema()

# COMMAND ----------

# Cleaning Orders for Silver Layer

from pyspark.sql.functions import col, trim

orders_silver_df = orders_bronze_df \
    .filter(
        col("OrderID").isNotNull() &
        col("CustomerID").isNotNull() &
        col("OrderDate").isNotNull()
    ) \
    .dropDuplicates(["OrderID"]) \
    .withColumn("OrderID", trim(col("OrderID"))) \
    .withColumn("CustomerID", trim(col("CustomerID"))) \
    .withColumn("SalespersonID", trim(col("SalespersonID"))) \
    .withColumn("OrderChannel", trim(col("OrderChannel"))) \
    .withColumn("Region", trim(col("Region")))

display(orders_silver_df)

# COMMAND ----------

# Saving Clean Orders to Silver Delta Table

orders_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.orders")

# COMMAND ----------

# Verifying Silver Orders Table

spark.sql("""
SELECT *
FROM retail_analytics.silver.orders
LIMIT 10
""").show()

# COMMAND ----------

# Loading Customers from Bronze Delta Table

customers_bronze_df = spark.table("retail_analytics.bronze.customers")

display(customers_bronze_df)

# COMMAND ----------

# Checking Customers Data Types

customers_bronze_df.printSchema()

# COMMAND ----------

# Cleaning Customers for Silver Layer

from pyspark.sql.functions import col, trim

customers_silver_df = customers_bronze_df \
    .filter(
        col("CustomerID").isNotNull() &
        col("CustomerName").isNotNull()
    ) \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("CustomerID", trim(col("CustomerID"))) \
    .withColumn("CustomerName", trim(col("CustomerName"))) \
    .withColumn("City", trim(col("City"))) \
    .withColumn("State", trim(col("State"))) \
    .withColumn("Segment", trim(col("Segment"))) \
    .withColumn("Region", trim(col("Region")))

display(customers_silver_df)

# COMMAND ----------

# Saving Clean Customers to Silver Delta Table

customers_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.customers")

# COMMAND ----------

# Loading Products from Bronze Delta Table

products_bronze_df = spark.table("retail_analytics.bronze.products")

display(products_bronze_df)

# COMMAND ----------

# Checking Products Data Types

products_bronze_df.printSchema()

# COMMAND ----------

# Cleaning Products for Silver Layer

from pyspark.sql.functions import col, trim

products_silver_df = products_bronze_df \
    .filter(
        col("ProductID").isNotNull() &
        col("ProductName").isNotNull()
    ) \
    .dropDuplicates(["ProductID"]) \
    .withColumn("ProductID", trim(col("ProductID"))) \
    .withColumn("ProductName", trim(col("ProductName"))) \
    .withColumn("Category", trim(col("Category"))) \
    .withColumn("SubCategory", trim(col("SubCategory")))

display(products_silver_df)

# COMMAND ----------

# Saving Clean Products to Silver Delta Table

products_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.products")

# COMMAND ----------

# Loading Salespersons from Bronze Delta Table

salespersons_bronze_df = spark.table(
    "retail_analytics.bronze.salespersons"
)

display(salespersons_bronze_df)

# COMMAND ----------

# Checking Salespersons Data Types

salespersons_bronze_df.printSchema()

# COMMAND ----------

# Cleaning Salespersons for Silver Layer

from pyspark.sql.functions import col, trim

salespersons_silver_df = salespersons_bronze_df \
    .filter(
        col("SalespersonID").isNotNull() &
        col("SalespersonName").isNotNull()
    ) \
    .dropDuplicates(["SalespersonID"]) \
    .withColumn("SalespersonID", trim(col("SalespersonID"))) \
    .withColumn("SalespersonName", trim(col("SalespersonName"))) \
    .withColumn("Territory", trim(col("Territory"))) \
    .withColumn("Manager", trim(col("Manager")))

display(salespersons_silver_df)

# COMMAND ----------

# Saving Clean Salespersons to Silver Delta Table

salespersons_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.salespersons")

# COMMAND ----------

# Loading Order Items from Bronze Delta Table

order_items_bronze_df = spark.table(
    "retail_analytics.bronze.order_items"
)

display(order_items_bronze_df)

# COMMAND ----------

# Checking Order Items Data Types

order_items_bronze_df.printSchema()

# COMMAND ----------

# Cleaning Order Items for Silver Layer

from pyspark.sql.functions import col, trim

order_items_silver_df = order_items_bronze_df \
    .filter(
        col("LineItemID").isNotNull() &
        col("OrderID").isNotNull() &
        col("ProductID").isNotNull()
    ) \
    .dropDuplicates(["LineItemID"]) \
    .withColumn("LineItemID", trim(col("LineItemID"))) \
    .withColumn("OrderID", trim(col("OrderID"))) \
    .withColumn("ProductID", trim(col("ProductID")))

display(order_items_silver_df)

# COMMAND ----------

# Saving Clean Order Items to Silver Delta Table

order_items_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.order_items")

# COMMAND ----------

# Verifying All Silver Tables

spark.sql("SHOW TABLES IN retail_analytics.silver").show()

# COMMAND ----------

# Loading Customers from Silver for Gold Layer

customers_silver_df = spark.table(
    "retail_analytics.silver.customers"
)

display(customers_silver_df)

# COMMAND ----------

# Creating Customer Dimension for Gold Layer

dim_customer_df = customers_silver_df.select(
    "CustomerID",
    "CustomerName",
    "City",
    "State",
    "Segment",
    "Region"
)

display(dim_customer_df)

# COMMAND ----------

# Saving Customer Dimension to Gold Delta Table

dim_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_customer")

# COMMAND ----------

# Loading Products from Silver for Gold Layer

products_silver_df = spark.table(
    "retail_analytics.silver.products"
)

display(products_silver_df)

# COMMAND ----------

# DBTITLE 1,Creating Product Dimension for Gold Layer
# Creating Product Dimension for Gold Layer

dim_product_df = products_silver_df.select(
    "ProductID",
    "ProductName",
    "Category",
    "SubCategory",
    "UnitPrice"
)

display(dim_product_df)

# COMMAND ----------

# Saving Product Dimension to Gold Delta Table

dim_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_product")

# COMMAND ----------

# Loading Salespersons from Silver for Gold Layer

salespersons_silver_df = spark.table(
    "retail_analytics.silver.salespersons"
)

display(salespersons_silver_df)

# COMMAND ----------

# Creating Salesperson Dimension for Gold Layer

dim_salesperson_df = salespersons_silver_df.select(
    "SalespersonID",
    "SalespersonName",
    "Territory",
    "Manager"
)

display(dim_salesperson_df)

# COMMAND ----------

# Saving Salesperson Dimension to Gold Delta Table

dim_salesperson_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_salesperson")

# COMMAND ----------

# Creating Date Dimension for Gold Layer

from pyspark.sql.functions import (
    col,
    min,
    max,
    sequence,
    explode,
    year,
    month,
    monthname,
    quarter,
    dayofmonth,
    dayofweek
)

date_range = orders_silver_df.select(
    min("OrderDate").alias("min_date"),
    max("OrderDate").alias("max_date")
).collect()[0]

dates_df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{date_range["min_date"]}'),
            to_date('{date_range["max_date"]}'),
            interval 1 day
        )
    ) AS Date
""")

dim_date_df = dates_df \
    .withColumn("Year", year("Date")) \
    .withColumn("Month", month("Date")) \
    .withColumn("MonthName", monthname("Date")) \
    .withColumn("Quarter", quarter("Date")) \
    .withColumn("Day", dayofmonth("Date")) \
    .withColumn("DayOfWeek", dayofweek("Date"))

display(dim_date_df)

# COMMAND ----------

# Saving Date Dimension to Gold Delta Table

dim_date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_date")

# COMMAND ----------

# Loading Order Items from Silver for Fact Table

order_items_silver_df = spark.table(
    "retail_analytics.silver.order_items"
)

display(order_items_silver_df)

# COMMAND ----------

# Loading Orders from Silver for Fact Table

orders_silver_df = spark.table(
    "retail_analytics.silver.orders"
)

display(orders_silver_df)

# COMMAND ----------

# Joining Orders with Order Items

from pyspark.sql.functions import col

fact_sales_df = order_items_silver_df.alias("oi") \
    .join(
        orders_silver_df.alias("o"),
        col("oi.OrderID") == col("o.OrderID"),
        "inner"
    ) \
    .select(
        col("oi.LineItemID"),
        col("oi.OrderID"),
        col("o.OrderDate"),
        col("o.CustomerID"),
        col("oi.ProductID"),
        col("o.SalespersonID"),
        col("o.OrderChannel"),
        col("o.Region"),
        col("oi.Quantity"),
        col("oi.UnitPrice"),
        col("oi.Discount"),
        col("oi.SalesAmount")
    )

display(fact_sales_df)

# COMMAND ----------

# Checking Fact Sales Schema

fact_sales_df.printSchema()

# COMMAND ----------

# Saving Fact Sales to Gold Delta Table

fact_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.fact_sales")

# COMMAND ----------

# Verifying All Gold Tables

spark.sql("SHOW TABLES IN retail_analytics.gold").show()

# COMMAND ----------

# Validating Gold Fact Sales

spark.sql("""
SELECT
    COUNT(*) AS Total_Line_Items,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales
""").show()

# COMMAND ----------

# DBTITLE 1,# Validating Sales by Region
# Validating Sales by Region

spark.sql("""
SELECT
    Region,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales
GROUP BY Region
ORDER BY Total_Sales DESC
""").show()

# COMMAND ----------

# Standardizing Region Values in Silver Orders

from pyspark.sql.functions import initcap

orders_silver_df = orders_silver_df \
    .withColumn("Region", initcap("Region"))

display(orders_silver_df.select("Region").distinct())

# COMMAND ----------

# Saving Standardized Orders to Silver Delta Table

orders_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.orders")

# COMMAND ----------

# Rebuilding Fact Sales with Standardized Region

fact_sales_df = order_items_silver_df.alias("oi") \
    .join(
        orders_silver_df.alias("o"),
        col("oi.OrderID") == col("o.OrderID"),
        "inner"
    ) \
    .select(
        col("oi.LineItemID"),
        col("oi.OrderID"),
        col("o.OrderDate"),
        col("o.CustomerID"),
        col("oi.ProductID"),
        col("o.SalespersonID"),
        col("o.OrderChannel"),
        col("o.Region"),
        col("oi.Quantity"),
        col("oi.UnitPrice"),
        col("oi.Discount"),
        col("oi.SalesAmount")
    )

display(fact_sales_df)

# COMMAND ----------

# Updating Gold Fact Sales with Standardized Region

fact_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.fact_sales")

# COMMAND ----------

# Revalidating Sales by Region

spark.sql("""
SELECT
    Region,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales
GROUP BY Region
ORDER BY Total_Sales DESC
""").show()

# COMMAND ----------

# Validating Sales by Product Category

spark.sql("""
SELECT
    p.Category,
    COUNT(DISTINCT f.OrderID) AS Total_Orders,
    SUM(f.Quantity) AS Total_Quantity,
    SUM(f.SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales f
INNER JOIN retail_analytics.gold.dim_product p
    ON f.ProductID = p.ProductID
GROUP BY p.Category
ORDER BY Total_Sales DESC
""").show()

# COMMAND ----------

# Standardizing Product Category Values in Silver

from pyspark.sql.functions import initcap

products_silver_df = products_silver_df \
    .withColumn("Category", initcap("Category"))

display(products_silver_df.select("Category").distinct())

# COMMAND ----------

# Saving Standardized Products to Silver Delta Table

products_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.products")

# COMMAND ----------

# Rebuilding Product Dimension with Standardized Categories

dim_product_df = products_silver_df.select(
    "ProductID",
    "ProductName",
    "Category",
    "SubCategory",
    "UnitPrice"
)

display(dim_product_df)

# COMMAND ----------

# Updating Gold Product Dimension with Standardized Categories

dim_product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_product")

# COMMAND ----------

# Revalidating Sales by Product Category

spark.sql("""
SELECT
    p.Category,
    COUNT(DISTINCT f.OrderID) AS Total_Orders,
    SUM(f.Quantity) AS Total_Quantity,
    SUM(f.SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales f
INNER JOIN retail_analytics.gold.dim_product p
    ON f.ProductID = p.ProductID
GROUP BY p.Category
ORDER BY Total_Sales DESC
""").show()

# COMMAND ----------

# Checking Order Channel Values

spark.sql("""
SELECT
    OrderChannel,
    COUNT(*) AS Record_Count,
    SUM(SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales
GROUP BY OrderChannel
ORDER BY Total_Sales DESC
""").show()

# COMMAND ----------

# Validating Sales and Discount Values

spark.sql("""
SELECT
    MIN(Quantity) AS Min_Quantity,
    MAX(Quantity) AS Max_Quantity,
    MIN(UnitPrice) AS Min_UnitPrice,
    MAX(UnitPrice) AS Max_UnitPrice,
    MIN(Discount) AS Min_Discount,
    MAX(Discount) AS Max_Discount,
    MIN(SalesAmount) AS Min_SalesAmount,
    MAX(SalesAmount) AS Max_SalesAmount
FROM retail_analytics.gold.fact_sales
""").show()

# COMMAND ----------

# Checking Zero Quantity Records

spark.sql("""
SELECT
    COUNT(*) AS Zero_Quantity_Records,
    SUM(SalesAmount) AS Sales_From_Zero_Quantity
FROM retail_analytics.gold.fact_sales
WHERE Quantity = 0
""").show()

# COMMAND ----------

# Inspecting Zero Quantity Sales Record

spark.sql("""
SELECT *
FROM retail_analytics.gold.fact_sales
WHERE Quantity = 0
""").show(truncate=False)

# COMMAND ----------

# Checking Original Bronze Order Item

spark.sql("""
SELECT *
FROM retail_analytics.bronze.order_items
WHERE LineItemID = 'LI000421'
""").show(truncate=False)

# COMMAND ----------

# Removing Invalid Zero Quantity Order Items

from pyspark.sql.functions import col, trim

order_items_silver_df = order_items_bronze_df \
    .filter(
        col("LineItemID").isNotNull() &
        col("OrderID").isNotNull() &
        col("ProductID").isNotNull() &
        (col("Quantity") > 0)
    ) \
    .dropDuplicates(["LineItemID"]) \
    .withColumn("LineItemID", trim(col("LineItemID"))) \
    .withColumn("OrderID", trim(col("OrderID"))) \
    .withColumn("ProductID", trim(col("ProductID")))

display(order_items_silver_df)

# COMMAND ----------

# Updating Silver Order Items After Data Quality Check

order_items_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.silver.order_items")

# COMMAND ----------

# Rebuilding Fact Sales After Order Item Data Quality Fix

fact_sales_df = order_items_silver_df.alias("oi") \
    .join(
        orders_silver_df.alias("o"),
        col("oi.OrderID") == col("o.OrderID"),
        "inner"
    ) \
    .select(
        col("oi.LineItemID"),
        col("oi.OrderID"),
        col("o.OrderDate"),
        col("o.CustomerID"),
        col("oi.ProductID"),
        col("o.SalespersonID"),
        col("o.OrderChannel"),
        col("o.Region"),
        col("oi.Quantity"),
        col("oi.UnitPrice"),
        col("oi.Discount"),
        col("oi.SalesAmount")
    )

display(fact_sales_df)

# COMMAND ----------

# Updating Gold Fact Sales After Data Quality Fix

fact_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.fact_sales")

# COMMAND ----------

# Revalidating Gold Fact Sales After Data Quality Fix

spark.sql("""
SELECT
    COUNT(*) AS Total_Line_Items,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales
""").show()

# COMMAND ----------

# Checking for Unmatched Products in Gold Fact Sales

spark.sql("""
SELECT COUNT(*) AS Unmatched_Product_Records
FROM retail_analytics.gold.fact_sales f
LEFT JOIN retail_analytics.gold.dim_product p
    ON f.ProductID = p.ProductID
WHERE p.ProductID IS NULL
""").show()

# COMMAND ----------

# Checking for Unmatched Customers in Gold Fact Sales

spark.sql("""
SELECT COUNT(*) AS Unmatched_Customer_Records
FROM retail_analytics.gold.fact_sales f
LEFT JOIN retail_analytics.gold.dim_customer c
    ON f.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL
""").show()

# COMMAND ----------

# Checking for Unmatched Salespersons in Gold Fact Sales

spark.sql("""
SELECT COUNT(*) AS Unmatched_Salesperson_Records
FROM retail_analytics.gold.fact_sales f
LEFT JOIN retail_analytics.gold.dim_salesperson s
    ON f.SalespersonID = s.SalespersonID
WHERE s.SalespersonID IS NULL
""").show()

# COMMAND ----------

# Finding Unmatched Salesperson Records

spark.sql("""
SELECT
    f.SalespersonID,
    COUNT(*) AS Record_Count,
    SUM(f.SalesAmount) AS Total_Sales
FROM retail_analytics.gold.fact_sales f
LEFT JOIN retail_analytics.gold.dim_salesperson s
    ON f.SalespersonID = s.SalespersonID
WHERE s.SalespersonID IS NULL
GROUP BY f.SalespersonID
ORDER BY Record_Count DESC
""").show()

# COMMAND ----------

# Inspecting Records with Missing Salesperson

spark.sql("""
SELECT
    LineItemID,
    OrderID,
    OrderDate,
    CustomerID,
    ProductID,
    SalespersonID,
    OrderChannel,
    Region,
    Quantity,
    UnitPrice,
    Discount,
    SalesAmount
FROM retail_analytics.gold.fact_sales
WHERE SalespersonID IS NULL
""").show(truncate=False)

# COMMAND ----------

# Replacing Missing Salesperson IDs with UNASSIGNED

from pyspark.sql.functions import coalesce, lit

fact_sales_df = fact_sales_df \
    .withColumn(
        "SalespersonID",
        coalesce(col("SalespersonID"), lit("UNASSIGNED"))
    )

display(
    fact_sales_df
    .filter(col("SalespersonID") == "UNASSIGNED")
)

# COMMAND ----------

# Adding UNASSIGNED Salesperson to Dimension

unassigned_salesperson_df = spark.createDataFrame([
    ("UNASSIGNED", "Unassigned", "Unassigned", "Unassigned")
], [
    "SalespersonID",
    "SalespersonName",
    "Territory",
    "Manager"
])

dim_salesperson_df = dim_salesperson_df.unionByName(
    unassigned_salesperson_df
)

display(dim_salesperson_df)

# COMMAND ----------

# Updating Gold Salesperson Dimension with UNASSIGNED

dim_salesperson_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.dim_salesperson")

# COMMAND ----------

# Updating Gold Fact Sales with UNASSIGNED Salesperson

fact_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_analytics.gold.fact_sales")

# COMMAND ----------

# Rechecking Salesperson Relationships

spark.sql("""
SELECT COUNT(*) AS Unmatched_Salesperson_Records
FROM retail_analytics.gold.fact_sales f
LEFT JOIN retail_analytics.gold.dim_salesperson s
    ON f.SalespersonID = s.SalespersonID
WHERE s.SalespersonID IS NULL
""").show()

# COMMAND ----------

# Final Gold Fact Sales Validation

spark.sql("""
SELECT
    COUNT(*) AS Total_Line_Items,
    COUNT(DISTINCT OrderID) AS Total_Orders,
    SUM(Quantity) AS Total_Quantity,
    SUM(SalesAmount) AS Total_Sales,
    MIN(OrderDate) AS First_Order_Date,
    MAX(OrderDate) AS Last_Order_Date
FROM retail_analytics.gold.fact_sales
""").show()