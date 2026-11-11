# Databricks notebook source
# ---
# title: Silver Layer - Data Cleaning & Transformation
# description: Clean Bronze data and create Silver table with derived columns
# ---

# DBTITLE 1 - Parameters
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# DBTITLE 1 - Import libraries
from pyspark.sql import functions as F

# DBTITLE 1 - Configuration
source_table = f"{catalog}.{schema}.sales_bronze"
target_table = f"{catalog}.{schema}.sales_silver"

print(f"📥 Source: {source_table}")
print(f"📤 Target: {target_table}")

# DBTITLE 1 - Read from Bronze
print("\n📖 Reading Bronze data...")
bronze_df = spark.read.table(source_table)
initial_count = bronze_df.count()
print(f"✓ Initial records: {initial_count:,}")

# DBTITLE 1 - Data cleaning and transformation
print("\n🔧 Cleaning and transforming data...")
silver_df = (
    bronze_df
    # Remove invalid records
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("date").isNotNull())
    
    # Calculate derived columns
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("sale_date", F.to_date("date"))
    .withColumn("sale_month", F.month("sale_date"))
    .withColumn("sale_year", F.year("sale_date"))
    .withColumn("sale_quarter", F.quarter("sale_date"))
    .withColumn("day_of_week", F.dayofweek("sale_date"))
    .withColumn("day_name", F.date_format("sale_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("sale_date"))
    
    # Categorize transactions
    .withColumn("transaction_size", 
        F.when(F.col("total_amount") < 50, "Small")
         .when((F.col("total_amount") >= 50) & (F.col("total_amount") < 200), "Medium")
         .when((F.col("total_amount") >= 200) & (F.col("total_amount") < 1000), "Large")
         .otherwise("Extra Large")
    )
    
    # Price category
    .withColumn("price_category",
        F.when(F.col("unit_price") < 25, "Budget")
         .when((F.col("unit_price") >= 25) & (F.col("unit_price") < 100), "Mid-range")
         .when((F.col("unit_price") >= 100) & (F.col("unit_price") < 500), "Premium")
         .otherwise("Luxury")
    )
    
    # Clean city names
    .withColumn("city", F.initcap(F.col("city")))
    
    # Add data quality flag
    .withColumn("is_valid",
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0) &
        (F.col("total_amount") > 0)
    )
)

final_count = silver_df.count()
print(f"✓ Final records: {final_count:,}")
print(f"✓ Records removed: {initial_count - final_count:,}")

# DBTITLE 1 - Write to Silver table
print("\n💾 Writing to Silver table...")
silver_df.write.mode("overwrite").saveAsTable(target_table)

# DBTITLE 1 - Validation
print("\n✅ Validation:")
validation_df = spark.read.table(target_table)
record_count = validation_df.count()

# Check required columns
required_columns = ["total_amount", "sale_date", "transaction_size", "price_category"]
for col in required_columns:
    assert col in validation_df.columns, f"Missing column: {col}"

print(f"✓ Total records: {record_count:,}")
print(f"✓ Total columns: {len(validation_df.columns)}")
print(f"✓ Silver table created: {target_table}")

# DBTITLE 1 - Data quality summary
print("\n📊 Data Quality Summary:")
validation_df.groupBy("is_valid").count().display()

print("\n📦 Transaction Size Distribution:")
validation_df.groupBy("transaction_size").count().orderBy("transaction_size").display()

print("\n💵 Price Category Distribution:")
validation_df.groupBy("price_category").count().orderBy("price_category").display()

# DBTITLE 1 - Display sample
display(validation_df.limit(10))
