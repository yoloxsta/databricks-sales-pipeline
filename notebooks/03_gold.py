# Databricks notebook source
# ---
# title: Gold Layer - Business Aggregations
# description: Create business-ready aggregated tables for analytics
# ---

# DBTITLE 1 - Parameters
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# DBTITLE 1 - Import libraries
from pyspark.sql import functions as F

# DBTITLE 1 - Configuration
source_table = f"{catalog}.{schema}.sales_silver"
gold_daily = f"{catalog}.{schema}.gold_daily_sales"
gold_category = f"{catalog}.{schema}.gold_category_sales"
gold_city = f"{catalog}.{schema}.gold_city_sales"

print(f"📥 Source: {source_table}")
print(f"📤 Targets:")
print(f"   - {gold_daily}")
print(f"   - {gold_category}")
print(f"   - {gold_city}")

# DBTITLE 1 - Read from Silver
print("\n📖 Reading Silver data...")
silver_df = spark.read.table(source_table)
print(f"✓ Records: {silver_df.count():,}")

# DBTITLE 1 - Create Daily Sales Aggregation
print("\n📊 Creating daily sales aggregation...")
gold_daily_df = (
    silver_df
    .filter(F.col("is_valid") == True)
    .groupBy("sale_date")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        F.round(F.min("total_amount"), 2).alias("min_transaction"),
        F.round(F.max("total_amount"), 2).alias("max_transaction"),
        F.sum("quantity").alias("total_items_sold"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_name").alias("unique_products")
    )
    .withColumn("day_name", F.date_format("sale_date", "EEEE"))
    .withColumn("month", F.month("sale_date"))
    .withColumn("year", F.year("sale_date"))
    .orderBy("sale_date")
)

gold_daily_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_daily)
print(f"✓ Daily sales table: {gold_daily}")
print(f"  Records: {gold_daily_df.count():,}")

# DBTITLE 1 - Create Category Performance
print("\n📦 Creating category performance aggregation...")
gold_category_df = (
    silver_df
    .filter(F.col("is_valid") == True)
    .groupBy("category")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        F.round(F.sum("quantity"), 2).alias("total_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("product_name").alias("unique_products"),
        F.countDistinct("city").alias("cities_sold")
    )
    .withColumn("revenue_percentage",
        F.round(
            (F.col("total_revenue") / gold_category_df.agg(F.sum("total_revenue")).collect()[0][0]) * 100,
            2
        )
    )
    .orderBy(F.col("total_revenue").desc())
)

gold_category_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_category)
print(f"✓ Category performance table: {gold_category}")
print(f"  Categories: {gold_category_df.count()}")

# DBTITLE 1 - Create City Sales
print("\n🏙️ Creating city sales aggregation...")
gold_city_df = (
    silver_df
    .filter(F.col("is_valid") == True)
    .groupBy("city")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        F.sum("quantity").alias("total_items_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("category").alias("categories_purchased")
    )
    .orderBy(F.col("total_revenue").desc())
)

gold_city_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_city)
print(f"✓ City sales table: {gold_city}")
print(f"  Cities: {gold_city_df.count()}")

# DBTITLE 1 - Summary Metrics
print("\n" + "="*50)
print("📊 GOLD LAYER SUMMARY")
print("="*50)

total_revenue = gold_daily_df.agg(F.sum("total_revenue")).collect()[0][0]
total_transactions = gold_daily_df.agg(F.sum("total_transactions")).collect()[0][0]
total_items = gold_daily_df.agg(F.sum("total_items_sold")).collect()[0][0]

print(f"💰 Total Revenue: ${total_revenue:,.2f}")
print(f"📈 Total Transactions: {total_transactions:,}")
print(f"📦 Total Items Sold: {total_items:,}")
print(f"💵 Avg Transaction: ${total_revenue/total_transactions:.2f}")

print("\n" + "="*50)
print("✅ GOLD LAYER COMPLETE")
print("="*50)

# DBTITLE 1 - Display results
print("\n📅 Daily Sales:")
display(gold_daily_df.limit(10))

print("\n📦 Category Performance:")
display(gold_category_df)

print("\n🏙️ City Performance:")
display(gold_city_df)
