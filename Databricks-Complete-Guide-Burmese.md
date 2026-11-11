# Databricks အပြည့်အစုံလမ်းညွှန် - စတင်သူများအတွက် သင်တန်းစာအုပ်

> **ရေးသားသည့်ရက်စွဲ**: 2026 ခုနှစ်၊ နိုဝင်ဘာလ  
> **ရည်ရွယ်ချက်**: Databricks ကို စတင်သူများအတွက် အခြေခံမှ အဆင့်မြင့်အထိ မြန်မာဘာသာဖြင့် ရှင်းပြခြင်း

---

## မာတိကာ

1. [Databricks ဆိုတာဘာလဲ - အခြေခံအယူအဆ](#1-databricks-ဆိုတာဘာလဲ)
2. [Databricks ဘာကြောင့်လိုအပ်လဲ - ပြဿနာနှင့် ဖြေရှင်းချက်](#2-databricks-ဘာကြောင့်လိုအပ်လဲ)
3. [Lakehouse Architecture - ခေတ်သစ် Data Platform](#3-lakehouse-architecture)
4. [Unity Catalog - Data Governance](#4-unity-catalog)
5. [Delta Lake - ယုံကြည်ရသော Storage Layer](#5-delta-lake)
6. [Medallion Architecture - Bronze Silver Gold](#6-medallion-architecture)
7. [Apache Spark - Big Data Processing Engine](#7-apache-spark)
8. [Databricks Notebooks - Interactive Development](#8-databricks-notebooks)
9. [Jobs & Workflows - Automation](#9-jobs--workflows)
10. [Git Integration & CI/CD - Production Practices](#10-git-integration--cicd)
11. [လက်တွေ့ Project ဥပမာ](#11-လက်တွေ့-project-ဥပမာ)
12. [နိဂုံး](#12-နိဂုံး)

---

# 1. Databricks ဆိုတာဘာလဲ

## 1.1 ရိုးရှင်းသော အဖြေ

**Databricks** ဆိုတာ **Data + AI Platform** တစ်ခုဖြစ်ပါတယ်။ 

ဒါကို နားလည်အောင် ဥပမာပေးရရင်:

```
传统方式:
├── Database (MySQL, PostgreSQL) - ဒေတာသိမ်းဖို့
├── ETL Tool (Informatica, Talend) - ဒေတာပြောင်းဖို့
├── BI Tool (Tableau, Power BI) - ဒေတာပြသဖို့
├── ML Platform (SageMaker) - Machine Learning လုပ်ဖို့
└── Notebook (Jupyter) - ဒေတာ analyze လုပ်ဖို့

Databricks:
└── တစ်ခုတည်း Platform မှာ အားလုံးရှိ! ✅
```

---

## 1.2 သမိုင်းကြောင်း

### မူလအစ

**2009 ခုနှစ်**: UC Berkeley တက္ကသိုလ်မှာ **Apache Spark** ကို စတင်ဖန်တီးခဲ့ပါတယ်။

**2013 ခုနှစ်**: Spark တည်ထောင်သူများဖြစ်ကြသော:
- **Matei Zaharia** (Spark 창시자)
- **Ali Ghodsi** (현재 CEO)
- **Ion Stoica**
- **Patrick Wendell**
- **Reynold Xin**
- **Andy Konwinski**

တို့က **Databricks Company** ကို တည်ထောင်ခဲ့ပါတယ်။

### ဖွံ့ဖြိုးတိုးတက်မှု

```
2013: Databricks တည်ထောင်
2015: Databricks Community Edition ထုတ်
2017: Azure Databricks (Microsoft နှင့်ပူးပေါင်း)
2020: Delta Lake Open Source
2021: Unity Catalog ထုတ်
2023: Databricks AI/BI
2025: Free Edition ထုတ်
```

---

## 1.3 Core Components

Databricks မှာ အဓိက Component များ:

### A. Compute

| Component | ရည်ရွယ်ချက် |
|-----------|--------------|
| **Serverless Compute** | Auto-managed, fast startup |
| **Job Compute** | Automated task execution |
| **SQL Warehouse** | SQL analytics & BI |
| **Classic Compute** | Custom VM configuration |

### B. Storage

| Component | အသုံးပြုပုံ |
|-----------|------------|
| **Delta Lake** | Default table format |
| **Unity Catalog Volumes** | File storage (CSV, JSON) |
| **Managed Tables** | Auto-managed storage |

### C. Development

| Component | အသုံးပြုပုံ |
|-----------|------------|
| **Notebooks** | Interactive coding |
| **Jobs** | Automated workflows |
| **Repos** | Git integration |

### D. Governance

| Component | ရည်ရွယ်ချက် |
|-----------|--------------|
| **Unity Catalog** | Permissions, lineage, auditing |
| **Access Controls** | Table/column level security |

---

## 1.4 ဘယ်သူတွေသုံးလဲ

| Role | အသုံးပြုပုံ |
|------|------------|
| **Data Engineer** | Build data pipelines |
| **Data Analyst** | Query data, create dashboards |
| **Data Scientist** | Train ML models |
| **ML Engineer** | Deploy & monitor models |
| **Platform Engineer** | Manage infrastructure |

---

# 2. Databricks ဘာကြောင့်လိုအပ်လဲ

## 2.1 ရှေ့ကနေ ပြဿနာများ

### ပြဿနာ ၁: Data Silos

```
Company Data:
├── Sales data in MySQL
├── Customer data in Salesforce
├── Logs in S3
├── IoT data in Kafka
└── Documents in SharePoint

ပြဿနာ:
❌ Data တွေ ချိတ်ဆက်ရခက်
❌ Report တွေ မတူညီ
❌ Analysis လုပ်ရ အချိန်ကြာ
```

### ပြဿနာ ၂: Technology Fragmentation

```
Data Stack:
├── Data Lake (S3, ADLS)
├── Data Warehouse (Snowflake, Redshift)
├── ETL Tool (Informatica)
├── Streaming (Kafka)
├── ML Platform (SageMaker)
└── BI Tool (Tableau)

ပြဿနာ:
❌ Tool တွေများ
❌ Cost တက်
❌ Team တွေ ခွဲခြား
❌ Integration ခက်
```

### ပြဿနာ ၃: Data Quality

```
Data Pipeline:
Source → ETL → Warehouse → BI

ပြဿနာ:
❌ Data quality စစ်မရ
❌ Error handling ခက်
❌ Lineage မသိ
❌ Audit trail မရှိ
```

---

## 2.2 Databricks ဖြေရှင်းချက်

### ဖြေရှင်းချက် ၁: Unified Platform

```
Before:
Data Lake + Warehouse + ETL + ML + BI
= 5 different tools

After:
Databricks = 1 platform ✅
= All-in-one solution
```

### ဖြေရှင်းချက် ၂: Lakehouse Architecture

```
Traditional:
Data Lake (Cheap, Flexible) ❌ No transactions
Data Warehouse (Fast, Reliable) ❌ Expensive, Limited

Lakehouse (Best of Both):
✅ Cheap storage
✅ ACID transactions
✅ Fast queries
✅ Schema enforcement
✅ Time travel
```

### ဖြေရှင်းချက် ၃: Built-in Governance

```
Unity Catalog provides:
✅ Centralized permissions
✅ Data lineage
✅ Audit logging
✅ Data discovery
✅ Compliance
```

---

## 2.3 Real-World Use Cases

### Use Case ၁: E-commerce Company

```
Problem:
- နေ့စဉ် Orders သန်းပေါင်းများ
- Real-time inventory လို
- Customer recommendations လို
- Fraud detection လို

Databricks Solution:
┌─────────────────────────────────────┐
│ Orders → Bronze → Silver → Gold     │
│         ↓                           │
│   Real-time dashboard               │
│         ↓                           │
│   ML recommendations                │
│         ↓                           │
│   Fraud alerts                      │
└─────────────────────────────────────┘
```

### Use Case ၂: Financial Services

```
Problem:
- Transaction data များ
- Regulatory compliance လို
- Risk analysis လို
- Audit trail လို

Databricks Solution:
┌─────────────────────────────────────┐
│ Transactions → Data Lakehouse       │
│         ↓                           │
│   Risk models                       │
│         ↓                           │
│   Compliance reports                │
│         ↓                           │
│   Audit logs                        │
└─────────────────────────────────────┘
```

### Use Case ၃: Healthcare

```
Problem:
- Patient data များ
- HIPAA compliance လို
- Research analytics လို
- Drug discovery လို

Databricks Solution:
┌─────────────────────────────────────┐
│ Patient Data → Secure Lakehouse     │
│         ↓                           │
│   Privacy controls                  │
│         ↓                           │
│   Research analytics                │
│         ↓                           │
│   ML for diagnostics                │
└─────────────────────────────────────┘
```

---

# 3. Lakehouse Architecture

## 3.1 Data Lake vs Data Warehouse vs Lakehouse

### Data Lake

```
What: Raw data storage (files)
Pros:
✅ စျေးသက်သာ
✅ Any format (CSV, JSON, Parquet)
✅ Unlimited scale

Cons:
❌ No transactions
❌ Schema enforcement မရှိ
❌ Quality control မရှိ
❌ Slow queries

Example: Amazon S3, Azure Data Lake
```

### Data Warehouse

```
What: Structured data storage (tables)
Pros:
✅ Fast SQL queries
✅ ACID transactions
✅ Schema enforcement
✅ BI-friendly

Cons:
❌ စျေးကြီး
❌ Structured data only
❌ Limited flexibility
❌ Not good for ML

Example: Snowflake, Redshift
```

### Lakehouse (Databricks Approach)

```
What: Best of both worlds
Pros:
✅ Cheap storage (like Data Lake)
✅ ACID transactions (like Warehouse)
✅ Schema enforcement
✅ Time travel
✅ ML-friendly
✅ Streaming support
✅ BI support

How:
Open file format (Parquet)
+ Transaction log (Delta Lake)
+ Unity Catalog (Governance)
= LAKEHOUSE ✅

Example: Databricks Delta Lake
```

---

## 3.2 Lakehouse Architecture Layers

```
┌──────────────────────────────────────────────┐
│           Presentation Layer                 │
│  BI Tools, Dashboards, ML Models             │
└──────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────┐
│           Compute Layer                      │
│  Serverless, SQL Warehouse, Jobs             │
└──────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────┐
│           Lakehouse Layer                    │
│  Bronze | Silver | Gold (Medallion)          │
│  Delta Tables + Unity Catalog                │
└──────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────┐
│           Storage Layer                      │
│  Cloud Object Storage (S3, ADLS, GCS)        │
└──────────────────────────────────────────────┘
```

---

## 3.3 Why Lakehouse is Better

### Cost Comparison

| Storage Type | Cost/TB/Month | Performance |
|--------------|---------------|-------------|
| Data Warehouse | $100-500 | Fast |
| Data Lake | $20-50 | Slow |
| **Lakehouse** | **$20-50** | **Fast** ✅ |

### Feature Comparison

| Feature | Data Lake | Warehouse | Lakehouse |
|---------|-----------|-----------|-----------|
| Cost | ✅ Low | ❌ High | ✅ Low |
| Performance | ❌ Slow | ✅ Fast | ✅ Fast |
| Flexibility | ✅ High | ❌ Low | ✅ High |
| Transactions | ❌ No | ✅ Yes | ✅ Yes |
| Schema | ❌ No | ✅ Yes | ✅ Yes |
| ML Support | ✅ Yes | ❌ No | ✅ Yes |
| Streaming | ✅ Yes | ❌ No | ✅ Yes |

---

# 4. Unity Catalog

## 4.1 Unity Catalog ဆိုတာဘာလဲ

**Unity Catalog** သည် Databricks ၏ **Data Governance Platform** ဖြစ်ပါတယ်။

### ရိုးရှင်းသော ဥပမာ

```
Without Unity Catalog:
Database 1: HR team manages
Database 2: Sales team manages
Database 3: Finance team manages
→ No central control
→ No audit trail
→ Security risks ❌

With Unity Catalog:
Unity Catalog (Central)
├── HR data
├── Sales data
└── Finance data
→ Centralized permissions
→ Full audit trail
→ Secure ✅
```

---

## 4.2 Unity Catalog Hierarchy

```
Metastore (Top Level - Account)
│
├── Catalog 1: workspace
│   ├── Schema: default
│   │   ├── Table: sales_bronze
│   │   ├── Table: sales_silver
│   │   └── Volume: raw_files
│   │
│   └── Schema: production
│       ├── Table: customers
│       └── Table: orders
│
└── Catalog 2: analytics
    └── Schema: reports
        └── Table: monthly_sales

Three-level namespace:
catalog.schema.object
Example: workspace.default.sales_bronze
```

---

## 4.3 Unity Catalog Features

### Feature ၁: Permissions Management

```sql
-- Grant permissions
GRANT SELECT ON TABLE workspace.default.sales TO data_analysts;
GRANT MODIFY ON TABLE workspace.default.sales TO data_engineers;

-- Revoke permissions
REVOKE SELECT ON TABLE workspace.default.sales FROM data_analysts;
```

### Feature ၂: Data Lineage

```
Source Table → Transformation → Target Table

Unity Catalog automatically tracks:
✅ Where data came from
✅ What transformations applied
✅ Who accessed data
✅ When it was accessed
```

### Feature ၃: Audit Logging

```
Every action is logged:
✅ Who accessed what
✅ When they accessed
✅ What they did
✅ From where

For compliance:
- GDPR
- HIPAA
- SOX
- PCI-DSS
```

---

## 4.4 Managed vs External Assets

### Managed Tables

```
Characteristics:
✅ Unity Catalog manages everything
✅ Auto-managed storage
✅ Auto-managed lifecycle
✅ Drop table → Data deleted

When to use:
- Most use cases
- Simple setup
- Full governance needed

Example:
CREATE TABLE workspace.default.sales_bronze;
```

### External Tables

```
Characteristics:
✅ Data in your cloud storage
✅ Unity Catalog manages metadata only
✅ You manage lifecycle
✅ Drop table → Data NOT deleted

When to use:
- Data in existing location
- Shared with other systems
- Custom lifecycle management

Example:
CREATE TABLE workspace.default.external_sales
LOCATION 's3://my-bucket/sales/';
```

---

## 4.5 Volumes

**Volumes** = File storage in Unity Catalog

```
When to use:
✅ CSV, JSON files
✅ Images, documents
✅ ML artifacts
✅ Logs, configs

Example:
/Volumes/workspace/default/raw_files/data.csv
```

```sql
-- Create volume
CREATE VOLUME workspace.default.raw_files;

-- List files
LIST '/Volumes/workspace/default/raw_files/';

-- Read CSV
SELECT * FROM csv.`/Volumes/workspace/default/raw_files/data.csv`;
```

---

# 5. Delta Lake

## 5.1 Delta Lake ဆိုတာဘာလဲ

**Delta Lake** = Parquet files + Transaction log

### ရိုးရှင်းသော ဥပမာ

```
Regular Parquet:
my_table/
├── part-001.parquet
├── part-002.parquet
└── part-003.parquet

❌ No transaction log
❌ No ACID guarantees
❌ No time travel

Delta Table:
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json  ← Transaction log
│   ├── 00000000000000000001.json
│   └── 00000000000000000002.json
├── part-001.parquet
├── part-002.parquet
└── part-003.parquet

✅ Full transaction history
✅ ACID transactions
✅ Time travel
```

---

## 5.2 ACID Transactions

### What is ACID?

```
A - Atomicity
    All or nothing
    Transaction အားလုံးအောင် သို့မဟုတ် အားလုံးပြန်စ

Example:
INSERT 100 records
Error on record 99
→ All 100 rolled back ✅

C - Consistency
    Data integrity maintained
    Data တူညီမှန်ကန်မှု

Example:
Schema: id INT, name STRING
Insert: id="abc"
→ Rejected ✅

I - Isolation
    Concurrent access safe
    တစ်ချိန်တည်း access လုပ်လည်း ဘေးမကျ

Example:
User A reading, User B writing
→ Both see consistent state ✅

D - Durability
    Committed data survives
    Commit ဖြစ်ပြီး data ပျောက်မသွား

Example:
Write data, power failure
→ Data still there ✅
```

---

## 5.3 Delta Lake Features

### Feature ၁: Time Travel

```sql
-- View table at specific version
SELECT * FROM sales VERSION AS OF 0;

-- View table at specific timestamp
SELECT * FROM sales TIMESTAMP AS OF '2026-11-11 10:00:00';

-- View history
DESCRIBE HISTORY sales;
```

### Feature ၂: Schema Enforcement

```sql
-- Table schema
CREATE TABLE sales (
  id INT,
  amount DOUBLE
);

-- Try to insert wrong schema
INSERT INTO sales VALUES ('abc', 100);
→ Error: Schema mismatch ✅

-- Schema evolution (controlled)
INSERT INTO sales VALUES (1, 100, 'new_column');
→ Error by default

-- Enable schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true;
INSERT INTO sales VALUES (1, 100, 'new_column');
→ Success, new column added ✅
```

### Feature ၃: MERGE (Upsert)

```sql
-- Update existing, insert new
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Feature ၄: OPTIMIZE

```sql
-- Compact small files
OPTIMIZE sales;

-- Z-order for better performance
OPTIMIZE sales
ZORDER BY (customer_id, date);
```

---

## 5.4 Why Delta Lake is Important

### Without Delta Lake:

```
Problems:
❌ Concurrent writes fail
❌ No rollback on errors
❌ Slow queries on small files
❌ No schema enforcement
❌ Can't query old versions
```

### With Delta Lake:

```
Solutions:
✅ ACID transactions
✅ Automatic rollback
✅ OPTIMIZE for performance
✅ Schema enforcement
✅ Time travel
✅ MERGE for upserts
✅ Streaming support
```

---

# 6. Medallion Architecture

## 6.1 အယူအဆ

**Medallion Architecture** = Bronze → Silver → Gold

### ဥပမာ: ရွှေချိန်သမား

```
Gold ရွှေထုတ်ပုံ:

မြေကြီး (Raw Material)
    ↓
ဖြတ်တောက်သန့်စင် (Cleaning)
    ↓
ရွှေစစ်စစ် (Pure Gold)

ဒီအတိုင်ပဲ Data ကို:
Bronze (Raw) → Silver (Clean) → Gold (Pure)
```

---

## 6.2 Bronze Layer

### ရည်ရွယ်ချက်

```
Bronze = Raw Data Layer

Characteristics:
✅ Source data as-is
✅ Append-only
✅ Full history
✅ Minimal transformation
✅ Audit trail

What to add:
- Ingestion timestamp
- Source file name
- Source system ID
- Batch/processing ID
```

### ဥပမာ

```python
# Bronze layer transformation
bronze_df = (
    raw_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("sales_data.csv"))
    .withColumn("batch_id", F.lit("batch_001"))
)

# Save
bronze_df.write.mode("append").saveAsTable("sales_bronze")
```

---

## 6.3 Silver Layer

### ရည်ရွယ်ချက်

```
Silver = Cleaned & Validated Data

Characteristics:
✅ Data quality checks
✅ Type casting
✅ Deduplication
✅ Null handling
✅ Standardization
✅ Business rules applied

What to do:
- Remove duplicates
- Handle nulls
- Validate data types
- Enrich with reference data
- Calculate derived fields
```

### ဥပမာ

```python
# Silver layer transformation
silver_df = (
    bronze_df
    .filter(F.col("quantity") > 0)
    .filter(F.col("price") > 0)
    .withColumn("total_amount", F.col("quantity") * F.col("price"))
    .withColumn("sale_date", F.to_date("date"))
    .dropDuplicates(["transaction_id"])
)

# Save
silver_df.write.mode("overwrite").saveAsTable("sales_silver")
```

---

## 6.4 Gold Layer

### ရည်ရွယ်ချက်

```
Gold = Business-Ready Aggregations

Characteristics:
✅ Aggregated data
✅ KPIs calculated
✅ Dimensions built
✅ Optimized for queries
✅ Dashboard-ready

What to create:
- Daily/weekly/monthly aggregations
- Category summaries
- Customer segments
- Performance metrics
```

### ဥပမာ

```python
# Gold layer aggregation
gold_daily_sales = (
    silver_df
    .groupBy("sale_date")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_transaction")
    )
)

# Save
gold_daily_sales.write.mode("overwrite").saveAsTable("gold_daily_sales")
```

---

## 6.5 Benefits of Medallion Architecture

### Benefit ၁: Data Quality Progression

```
Bronze: Raw, may have errors
   ↓
Silver: Cleaned, validated
   ↓
Gold: Business-ready, trusted
```

### Benefit ၂: Reusability

```
Bronze → Multiple Silver tables
Silver → Multiple Gold tables

Example:
Bronze: raw_sales
   ├→ Silver: sales_clean
   ├→ Silver: sales_inventory
   └→ Silver: sales_returns

Silver: sales_clean
   ├→ Gold: daily_sales
   ├→ Gold: monthly_sales
   └→ Gold: category_sales
```

### Benefit ၃: Traceability

```
Find issues easily:
Gold error → Check Silver
Silver error → Check Bronze
Bronze error → Check source

Full lineage tracked ✅
```

---

# 7. Apache Spark

## 7.1 Apache Spark ဆိုတာဘာလဲ

**Apache Spark** = Fast, distributed data processing engine

### ရိုးရှင်းသော ဥပမာ

```
Single Computer:
1 file, 1 GB → Process in 10 seconds

Big Data:
1000 files, 1 TB → Single computer too slow ❌

Apache Spark:
1000 computers work together
→ Process in 10 seconds ✅
```

---

## 7.2 Spark Architecture

```
┌─────────────────────────────────────┐
│           Driver Program            │
│  (SparkSession - Your code)         │
│  - Coordinates execution            │
│  - Creates execution plans          │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│           Cluster Manager           │
│  (Databricks manages this)          │
│  - Allocates resources              │
│  - Manages worker nodes             │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│           Executor Nodes            │
│  - Process data partitions          │
│  - Run tasks in parallel            │
│  - Store intermediate results       │
└─────────────────────────────────────┘
```

---

## 7.3 Key Concepts

### Concept ၁: DataFrame

```python
# DataFrame = Distributed table with schema
df = spark.read.table("sales")

# Like a SQL table or Pandas DataFrame
# But distributed across multiple machines
```

### Concept ၂: Partition

```
Large table split into partitions:

Table: 1 billion rows
Partitioned into: 1000 partitions
Each partition: 1 million rows

Parallel processing:
1000 partitions → 1000 tasks → Run simultaneously ✅
```

### Concept ၃: Lazy Evaluation

```python
# Transformations (lazy)
df1 = df.filter(col("amount") > 100)  # Not executed yet
df2 = df1.groupBy("category").count()  # Not executed yet

# Action (triggers execution)
df2.show()  # NOW it runs! ✅
```

### Concept ၄: Actions vs Transformations

```
Transformations (Lazy):
- filter(), select(), groupBy()
- withColumn(), join(), union()
- Create execution plan, don't execute

Actions (Eager):
- show(), count(), collect()
- write(), saveAsTable()
- Trigger execution
```

---

## 7.4 Common Operations

### Reading Data

```python
# Read from table
df = spark.read.table("workspace.default.sales")

# Read CSV
df = spark.read.csv("/path/to/file.csv", header=True)

# Read JSON
df = spark.read.json("/path/to/file.json")

# Read Parquet
df = spark.read.parquet("/path/to/file.parquet")
```

### Transformations

```python
from pyspark.sql import functions as F

# Filter
df = df.filter(F.col("amount") > 100)

# Select columns
df = df.select("id", "name", "amount")

# Add column
df = df.withColumn("tax", F.col("amount") * 0.05)

# Group by
df_grouped = df.groupBy("category").agg(
    F.count("*").alias("count"),
    F.sum("amount").alias("total")
)

# Join
df_joined = df1.join(df2, "customer_id", "inner")

# Sort
df_sorted = df.orderBy(F.col("amount").desc())
```

### Writing Data

```python
# Write to table
df.write.mode("overwrite").saveAsTable("target_table")

# Write to file
df.write.mode("overwrite").parquet("/path/to/output/")
```

---

## 7.5 Spark SQL

```python
# Register temp view
df.createOrReplaceTempView("sales_temp")

# Run SQL
result = spark.sql("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(amount) as total
    FROM sales_temp
    GROUP BY category
    ORDER BY total DESC
""")

result.show()
```

---

# 8. Databricks Notebooks

## 8.1 Notebook ဆိုတာဘာလဲ

**Notebook** = Interactive document with code, results, and visualizations

```
Notebook contains:
┌────────────────────────────┐
│ Cell 1: Code (Python/SQL)  │
│ Output: Results/Table      │
├────────────────────────────┤
│ Cell 2: Markdown text      │
│ Output: Formatted text     │
├────────────────────────────┤
│ Cell 3: SQL query          │
│ Output: Chart/Graph        │
└────────────────────────────┘
```

---

## 8.2 Notebook Features

### Feature ၁: Multiple Languages

```python
# Python cell
df = spark.read.table("sales")
display(df)
```

```sql
%sql
-- SQL cell (use %sql magic)
SELECT * FROM sales LIMIT 10;
```

```scala
%scala
// Scala cell
val df = spark.table("sales")
display(df)
```

### Feature ၂: Visualizations

```python
# Create visualization from DataFrame
display(df)

# Options:
# - Bar chart
# - Line chart
# - Scatter plot
# - Pie chart
# - Map
# - Table
```

### Feature ၃: Widgets (Parameters)

```python
# Create parameter widgets
dbutils.widgets.text("date", "2026-11-11")
dbutils.widgets.dropdown("category", "Electronics", ["Electronics", "Furniture", "Office"])

# Use in code
date = dbutils.widgets.get("date")
category = dbutils.widgets.get("category")

df = spark.table("sales").filter(F.col("date") == date)
```

---

## 8.3 Notebook Best Practices

### Practice ၁: Modular Code

```python
# Bad: One giant cell with all code

# Good: Split into logical cells

# Cell 1: Configuration
catalog = "workspace"
schema = "default"

# Cell 2: Read data
bronze_df = spark.read.table(f"{catalog}.{schema}.sales_bronze")

# Cell 3: Transform
silver_df = bronze_df.filter(F.col("amount") > 0)

# Cell 4: Write
silver_df.write.saveAsTable(f"{catalog}.{schema}.sales_silver")
```

### Practice ၂: Add Documentation

```python
# DBTITLE 1 - Bronze Layer: Raw Data Ingestion
# This cell reads raw CSV data and creates Bronze table
# Input: CSV file from volume
# Output: Bronze table with metadata columns

bronze_df = (
    spark.read.csv(source_path)
    .withColumn("ingested_at", F.current_timestamp())
)
```

### Practice ၃: Error Handling

```python
try:
    df = spark.read.table("sales")
    assert df.count() > 0, "No data in table"
    print("✓ Data loaded successfully")
except Exception as e:
    print(f"❌ Error: {e}")
```

---

# 9. Jobs & Workflows

## 9.1 Jobs ဆိုတာဘာလဲ

**Job** = Automated, scheduled execution of tasks

```
Manual:
You open notebook → Click Run → Wait → Check results

Automated Job:
Scheduled automatically → Runs → Alerts you on completion
```

---

## 9.2 Job Components

### Task

```
Job: Sales ETL Pipeline
│
├── Task 1: bronze_task
│   - Notebook: 01_bronze.py
│   - Parameters: catalog="workspace"
│
├── Task 2: silver_task (depends on Task 1)
│   - Notebook: 02_silver.py
│
└── Task 3: gold_task (depends on Task 2)
    - Notebook: 03_gold.py
```

### Schedule

```sql
-- Daily at 6 AM
CRON: 0 0 6 * * ?

-- Every hour
CRON: 0 0 * * * ?

-- Every 15 minutes
CRON: 0 0/15 * * * ?
```

### Notifications

```
Configure alerts:
✅ Email on success
✅ Email on failure
✅ Webhook notifications
✅ Slack/Teams integration
```

---

## 9.3 Creating Jobs

### Via UI

```
1. Workflows → Jobs → Create Job
2. Add tasks:
   - Name: bronze_task
   - Type: Notebook
   - Path: notebooks/01_bronze.py
   - Parameters: catalog=workspace
3. Add dependencies
4. Set schedule
5. Configure notifications
6. Create
```

### Via API

```python
import requests

job_config = {
    "name": "Sales ETL Pipeline",
    "tasks": [
        {
            "task_key": "bronze_task",
            "notebook_task": {
                "notebook_path": "notebooks/01_bronze.py",
                "base_parameters": {
                    "catalog": "workspace",
                    "schema": "default"
                }
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "Asia/Yangon"
    }
}

response = requests.post(
    f"{DATABRICKS_HOST}/api/2.1/jobs/create",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json=job_config
)
```

---

# 10. Git Integration & CI/CD

## 10.1 Git Integration ဆိုတာဘာလဲ

**Git Integration** = Version control for Databricks notebooks

```
Without Git:
- Notebooks live only in Databricks
- No version history
- No code review
- Hard to collaborate ❌

With Git:
- Notebooks in GitHub/GitLab
- Full version history
- Pull requests & reviews
- Team collaboration ✅
```

---

## 10.2 Setting Up Git Integration

### Step 1: Create Git Repository

```bash
# Initialize repository
git init databricks-project
cd databricks-project

# Create structure
mkdir notebooks tests config
touch README.md .gitignore databricks.yml

# Create notebooks
touch notebooks/01_bronze.py
touch notebooks/02_silver.py
touch notebooks/03_gold.py
```

### Step 2: Push to GitHub

```bash
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/user/databricks-project.git
git push -u origin main
```

### Step 3: Connect to Databricks

```
1. Databricks → Workspace
2. Create Git folder
3. Enter repository URL
4. Authenticate
5. Clone complete ✅
```

---

## 10.3 CI/CD Pipeline

### What is CI/CD?

```
CI (Continuous Integration):
- Automatic testing on every push
- Validate code quality
- Run unit tests

CD (Continuous Deployment):
- Automatic deployment
- Update Databricks resources
- Trigger jobs
```

### GitHub Actions Workflow

```yaml
# .github/workflows/ci-cd.yml
name: Databricks CI/CD

on:
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Run tests
        run: |
          pip install pytest
          pytest tests/
      
      - name: Check syntax
        run: |
          python -m py_compile notebooks/*.py
  
  deploy:
    needs: validate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Trigger job
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          DATABRICKS_JOB_ID: ${{ secrets.DATABRICKS_JOB_ID }}
        run: |
          pip install databricks-cli
          databricks jobs run-now --job-id $DATABRICKS_JOB_ID
```

---

## 10.4 Development Workflow

```
Developer Workflow:

1. Create feature branch
   git checkout -b feature/new-metric

2. Make changes locally
   vim notebooks/03_gold.py

3. Test locally
   pytest tests/

4. Commit & push
   git add .
   git commit -m "Add new metric"
   git push origin feature/new-metric

5. Create Pull Request on GitHub
   → CI runs tests automatically
   → Team reviews code

6. Merge to main
   → CI runs tests again
   → CD triggers Databricks job
   → Pipeline runs automatically ✅
```

---

# 11. လက်တွေ့ Project ဥပမာ

## 11.1 Sales ETL Pipeline

### Project Overview

```
Goal: Build automated sales analytics pipeline

Data Flow:
CSV File → Bronze → Silver → Gold → Dashboard

Technologies:
- Databricks (Compute & Storage)
- Delta Lake (Data format)
- Unity Catalog (Governance)
- GitHub (Version control)
- GitHub Actions (CI/CD)
```

---

## 11.2 Project Structure

```
databricks-sales-pipeline/
├── README.md
├── databricks.yml
├── .github/
│   └── workflows/
│       └── ci-cd.yml
├── notebooks/
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   └── 03_gold.py
└── tests/
    └── test_notebooks.py
```

---

## 11.3 Bronze Layer Implementation

```python
# notebooks/01_bronze.py

# Parameters
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("source_file", "sales_data.csv")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_file = dbutils.widgets.get("source_file")

from pyspark.sql import functions as F

# Configuration
source_path = f"/Volumes/{catalog}/{schema}/raw_files/{source_file}"
target_table = f"{catalog}.{schema}.sales_bronze"

# Read source data
sales_df = spark.read.csv(source_path, header=True, inferSchema=True)

# Add metadata
bronze_df = (
    sales_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit(source_file))
    .withColumn("ingestion_date", F.current_date())
)

# Write to Bronze table
bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

# Validate
print(f"✓ Bronze table created: {target_table}")
print(f"✓ Records: {bronze_df.count():,}")
```

---

## 11.4 Silver Layer Implementation

```python
# notebooks/02_silver.py

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

source_table = f"{catalog}.{schema}.sales_bronze"
target_table = f"{catalog}.{schema}.sales_silver"

# Read from Bronze
bronze_df = spark.read.table(source_table)

# Clean and transform
silver_df = (
    bronze_df
    # Remove invalid records
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    
    # Calculate derived columns
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("sale_date", F.to_date("date"))
    .withColumn("sale_month", F.month("sale_date"))
    .withColumn("sale_year", F.year("sale_date"))
    
    # Categorize transactions
    .withColumn("transaction_size", 
        F.when(F.col("total_amount") < 50, "Small")
         .when(F.col("total_amount") < 200, "Medium")
         .when(F.col("total_amount") < 1000, "Large")
         .otherwise("Extra Large")
    )
)

# Write to Silver table
silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"✓ Silver table created: {target_table}")
```

---

## 11.5 Gold Layer Implementation

```python
# notebooks/03_gold.py

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

silver_df = spark.read.table(f"{catalog}.{schema}.sales_silver")

# Daily sales aggregation
gold_daily_sales = (
    silver_df
    .groupBy("sale_date")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        F.sum("quantity").alias("total_items_sold")
    )
    .orderBy("sale_date")
)

gold_daily_sales.write.mode("overwrite").option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog}.{schema}.gold_daily_sales")

# Category performance
gold_category_sales = (
    silver_df
    .groupBy("category")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

gold_category_sales.write.mode("overwrite").option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog}.{schema}.gold_category_sales")

print("✓ Gold tables created!")
```

---

# 12. နိဂုံး

## 12.1 သင်ယူခဲ့သည်များ

```
✅ Databricks ဆိုတာဘာလဲ
✅ Lakehouse Architecture
✅ Unity Catalog & Governance
✅ Delta Lake & ACID transactions
✅ Medallion Architecture (Bronze/Silver/Gold)
✅ Apache Spark fundamentals
✅ Notebooks & development
✅ Jobs & automation
✅ Git integration & CI/CD
✅ Real-world project implementation
```

---

## 12.2 နောက်ထပ်သင်ယူရန်များ

### Advanced Topics

```
📚 Next steps:
1. Structured Streaming
2. Machine Learning with MLflow
3. Delta Live Tables (DLT)
4. Unity Catalog advanced features
5. Performance optimization
6. Security & networking
7. Multi-environment setup
```

---

## 12.3 Resources

### Official Documentation

```
🔗 Databricks Documentation
https://docs.databricks.com/

🔗 Azure Databricks
https://learn.microsoft.com/azure/databricks/

🔗 Delta Lake
https://delta.io/

🔗 Apache Spark
https://spark.apache.org/
```

### Learning Path

```
1. Start with: Databricks Fundamentals
2. Learn: Delta Lake basics
3. Practice: Build Bronze/Silver/Gold pipeline
4. Master: CI/CD & automation
5. Advanced: MLflow & streaming
```

---

## 12.4 Tips for Success

### Development Tips

```
✅ Start small, iterate
✅ Test in dev environment first
✅ Use version control (Git)
✅ Write tests for your code
✅ Document everything
✅ Monitor job performance
✅ Set up alerts
```

### Production Tips

```
✅ Use parameterized notebooks
✅ Implement error handling
✅ Set up proper permissions
✅ Monitor data quality
✅ Create runbooks
✅ Have a rollback plan
```

---

## 12.5 Closing

**Databricks** သည် modern data platform တစ်ခုဖြစ်ပြီး:

- Data engineering
- Data analytics
- Machine learning
- Business intelligence

တို့ကို တစ်နေရာတည်းမှာ လုပ်ဆောင်နိုင်ပါတယ်။

**Lakehouse Architecture** ကို အသုံးပြုခြင်းအားဖြင့်:
- Data Lake ၏ flexibility
- Data Warehouse ၏ performance
- နှစ်ခုလုံး၏ benefits ကို ရရှိနိုင်ပါတယ်။

**Medallion Architecture** (Bronze → Silver → Gold) က:
- Data quality ကို တိုးတက်စေ
- Data lineage ကို လိုက်လံသိရ
- Reusability ကို ပေါ်လာစေ

**Unity Catalog** က:
- Governance ကို centralized လုပ်
- Permissions ကို ထိန်းသိမ်း
- Audit trail ကို ထား

**Delta Lake** က:
- ACID transactions ပေး
- Time travel လုပ်နိုင်
- Schema enforcement လုပ်

**Apache Spark** က:
- Big data processing
- Distributed computing
- Fast performance

**CI/CD** က:
- Automated testing
- Automated deployment
- Production-ready

ဒီအားလုံးကို ပေါင်းစပ်ပြီး သင့် data pipeline ကို professional level မှာ တည်ဆောက်နိုင်ပါပြီ! 🎉

---

**Author**: Databricks Learning Project  
**Language**: Myanmar (Burmese)  
**Date**: November 2026

---

## 📚 Appendix: Glossary

| Term | Burmese Translation | Definition |
|------|---------------------|------------|
| Data Lake | ဒေတာရေကန် | Raw data storage |
| Data Warehouse | ဒေတာဂိုထောက် | Structured data storage |
| Lakehouse | လိတ်ဟောက်စ် | Combined Lake + Warehouse |
| Bronze Layer | ကြေးနီအလွှာ | Raw data layer |
| Silver Layer | ငွေအလွှာ | Cleaned data layer |
| Gold Layer | ရွှေအလွှာ | Business-ready layer |
| Partition | အပိုင်းခွဲ | Data split for parallel processing |
| Schema | စီမံခန့်ခွဲမှု | Data structure definition |
| ACID | ACID | Transaction guarantees |
| CI/CD | CI/CD | Automation pipeline |
| DataFrame | ဒေတာဖရိမ် | Distributed data table |
| Notebook | နုတ်ဘွတ်ခ် | Interactive code document |
| Job | အလုပ် | Automated task execution |
| Cluster | ကလပ်စတာ | Group of computers |
| Executor | လုပ်ဆောင်သူ | Task execution node |
| Driver | မောင်းနှင်သူ | Main control program |
