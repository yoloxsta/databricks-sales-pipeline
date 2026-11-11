# Databricks notebook source
# ---
# title: Bronze Layer - Raw Data Ingestion
# description: Read CSV from Volume and create Bronze table with metadata
# ---

# DBTITLE 1 - Parameters (can be overridden by job)
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("source_file", "sales_data.csv")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_file = dbutils.widgets.get("source_file")

# DBTITLE 1 - Import libraries
from pyspark.sql import functions as F
from datetime import datetime

# DBTITLE 1 - Configuration
source_path = f"/Volumes/{catalog}/{schema}/test/{source_file}"
target_table = f"{catalog}.{schema}.sales_bronze"

print(f"📁 Source: {source_path}")
print(f"📊 Target: {target_table}")

# DBTITLE 1 - Read source CSV
print("\n📖 Reading source data...")
sales_df = spark.read.csv(
    source_path,
    header=True,
    inferSchema=True
)

print(f"✓ Records read: {sales_df.count():,}")
print(f"✓ Columns: {len(sales_df.columns)}")

# DBTITLE 1 - Add metadata columns
print("\n🔧 Adding metadata columns...")
bronze_df = (
    sales_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit(source_file))
    .withColumn("ingestion_date", F.current_date())
)

# DBTITLE 1 - Write to Bronze table
print("\n💾 Writing to Bronze table...")
bronze_df.write.mode("overwrite").saveAsTable(target_table)

# DBTITLE 1 - Validation
print("\n✅ Validation:")
validation_df = spark.read.table(target_table)
record_count = validation_df.count()
column_count = len(validation_df.columns)

assert "ingested_at" in validation_df.columns, "Missing ingested_at column"
assert "source_file" in validation_df.columns, "Missing source_file column"
assert record_count > 0, "No records in table"

print(f"✓ Total records: {record_count:,}")
print(f"✓ Total columns: {column_count}")
print(f"✓ Bronze table created: {target_table}")

# DBTITLE 1 - Display sample
display(validation_df.limit(5))

# DBTITLE 1 - Summary statistics
print("\n📊 Summary:")
validation_df.summary().display()
