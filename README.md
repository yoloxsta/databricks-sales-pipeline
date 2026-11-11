# Databricks Sales Pipeline Project

> **မြန်မာဘာသာဖြင့် အပြည့်အစုံလေ့လာရေးလမ်းညွှန်**: [Databricks-Complete-Guide-Burmese.md](./Databricks-Complete-Guide-Burmese.md)

Production-ready ETL pipeline using Databricks with Medallion Architecture (Bronze → Silver → Gold).

---

## 🎯 Project Overview

This project demonstrates a complete data pipeline using:

- **Databricks**: Unified analytics platform
- **Delta Lake**: Reliable storage layer
- **Unity Catalog**: Data governance
- **Medallion Architecture**: Bronze/Silver/Gold layers
- **Git Integration**: Version control
- **CI/CD**: Automated testing & deployment

---

## 📁 Project Structure

```
databricks-sales-pipeline/
├── Databricks-Complete-Guide-Burmese.md  # 📘 Full theory guide (Burmese)
├── README.md                               # This file
├── databricks.yml                          # Databricks bundle config
├── CI-CD-GUIDE.md                         # CI/CD setup instructions
├── .github/
│   └── workflows/
│       └── ci-cd.yml                       # GitHub Actions workflow
├── notebooks/
│   ├── 01_bronze.py                       # Raw data ingestion
│   ├── 02_silver.py                       # Data cleaning & transformation
│   └── 03_gold.py                         # Business aggregations
└── tests/
    └── test_notebooks.py                   # Unit tests
```

---

## 🏗️ Architecture

### Medallion Architecture

```
CSV File (Volume)
      │
      ▼
┌─────────────────┐
│  Bronze Layer   │  Raw data + metadata
│  sales_bronze   │  - ingested_at
└─────────────────┘  - source_file
      │
      ▼
┌─────────────────┐
│  Silver Layer   │  Cleaned & transformed
│  sales_silver   │  - total_amount
└─────────────────┘  - transaction_size
      │              - price_category
      ▼
┌─────────────────┐
│   Gold Layer    │  Business aggregations
├─────────────────┤
│ gold_daily_sales│  Daily metrics
│gold_category_sal│  Category performance
│ gold_city_sales │  City breakdown
└─────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Serverless | Auto-scaling execution |
| **Storage** | Delta Lake | ACID transactions |
| **Governance** | Unity Catalog | Permissions & lineage |
| **Version Control** | Git + GitHub | Code management |
| **CI/CD** | GitHub Actions | Automation |
| **Language** | Python (PySpark) | Data processing |

---

## 🚀 Quick Start

### Prerequisites

- Databricks workspace (Free Edition or Azure)
- GitHub account
- Git installed locally

### Setup Steps

#### 1. Clone Repository

```bash
git clone https://github.com/yoloxsta/databricks-sales-pipeline.git
cd databricks-sales-pipeline
```

#### 2. Connect to Databricks

1. Go to Databricks → Workspace
2. Click **Create Git folder**
3. Enter repository URL
4. Authenticate with GitHub
5. Clone complete ✅

#### 3. Upload Sample Data

1. Create volume:
   ```sql
   CREATE VOLUME workspace.default.raw_files;
   ```

2. Upload CSV to volume via UI

#### 4. Run Notebooks

Run notebooks in order:
1. `notebooks/01_bronze.py`
2. `notebooks/02_silver.py`
3. `notebooks/03_gold.py`

#### 5. Create Automated Job

1. Go to **Workflows** → **Jobs** → **Create Job**
2. Add 3 tasks (Bronze → Silver → Gold)
3. Set dependencies
4. Add schedule (optional)

---

## 📊 Data Pipeline

### Bronze Layer

**Purpose**: Raw data with metadata

**Input**: CSV file from volume

**Output**: `workspace.default.sales_bronze`

**Transformations**:
- Read CSV with schema inference
- Add ingestion timestamp
- Add source file name
- Add ingestion date

### Silver Layer

**Purpose**: Cleaned and validated data

**Input**: `sales_bronze` table

**Output**: `workspace.default.sales_silver`

**Transformations**:
- Filter invalid records (quantity > 0, price > 0)
- Calculate `total_amount = quantity × unit_price`
- Extract date components (month, year, quarter)
- Categorize transactions (Small/Medium/Large/Extra Large)
- Classify price category (Budget/Mid-range/Premium/Luxury)

### Gold Layer

**Purpose**: Business-ready aggregations

**Input**: `sales_silver` table

**Output**: 3 gold tables

**Tables**:
1. `gold_daily_sales` - Daily aggregated metrics
2. `gold_category_sales` - Category performance
3. `gold_city_sales` - City-wise breakdown

---

## 🔧 Configuration

### Notebook Parameters

All notebooks accept parameters:

```python
catalog = dbutils.widgets.get("catalog")      # Default: "workspace"
schema = dbutils.widgets.get("schema")        # Default: "default"
source_file = dbutils.widgets.get("source_file")  # Default: "sales_data.csv"
```

### Job Configuration

Edit `databricks.yml` to customize:

```yaml
resources:
  jobs:
    sales_etl_job:
      name: Sales ETL Pipeline
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # Daily 6 AM
        timezone_id: "Asia/Yangon"
```

---

## 🧪 Testing

### Run Tests Locally

```bash
# Install pytest
pip install pytest

# Run all tests
pytest tests/test_notebooks.py -v

# Run specific test
pytest tests/test_notebooks.py::test_bronze_notebook_syntax -v
```

### Test Coverage

- ✅ Python syntax validation
- ✅ Required imports checking
- ✅ Parameter validation
- ✅ Data flow validation
- ✅ Schema structure tests

---

## � CI/CD Pipeline

### Automated Workflow

```
Push to GitHub
    ↓
GitHub Actions
    ├── ✅ Validate code syntax
    ├── ✅ Run unit tests
    ├── ✅ Check code quality
    └── ✅ Trigger Databricks job
    ↓
Databricks Job Runs
    ├── bronze_task
    ├── silver_task
    └── gold_task
    ↓
Success! ✅
```

### Setup CI/CD

1. **Add GitHub Secrets**:
   - `DATABRICKS_HOST`: Your workspace URL
   - `DATABRICKS_TOKEN`: Access token
   - `DATABRICKS_JOB_ID`: Job ID (736391739459463)

2. **Push to GitHub**:
   ```bash
   git add .
   git commit -m "Update code"
   git push
   ```

3. **Watch Automation**:
   - Go to GitHub Actions tab
   - See tests run automatically
   - Check Databricks for job execution

---

## 📈 Monitoring

### Job Monitoring

- View job runs: Workflows → Jobs → [Your Job]
- Check run history
- Review logs
- Monitor duration

### Data Quality

```python
# Check record counts
bronze_count = spark.read.table("sales_bronze").count()
silver_count = spark.read.table("sales_silver").count()
gold_count = spark.read.table("gold_daily_sales").count()

# Validate data quality
assert silver_count <= bronze_count, "Silver should have ≤ Bronze records"
assert gold_count > 0, "Gold should have records"
```

---

## 📚 Documentation

### Burmese Guide

**Complete theory guide in Burmese**: [Databricks-Complete-Guide-Burmese.md](./Databricks-Complete-Guide-Burmese.md)

Topics covered:
- Databricks fundamentals
- Lakehouse architecture
- Unity Catalog
- Delta Lake
- Medallion architecture
- Apache Spark
- Notebooks
- Jobs & workflows
- Git & CI/CD
- Real-world examples

### CI/CD Guide

**Setup instructions**: [CI-CD-GUIDE.md](./CI-CD-GUIDE.md)

---

## 🛠️ Troubleshooting

### Common Issues

**Issue 1**: `NameError: name 'raw_df' is not defined`
- **Cause**: Previous cell not executed
- **Fix**: Run all cells from beginning

**Issue 2**: Schema mismatch error
- **Cause**: Table schema changed
- **Fix**: Add `.option("overwriteSchema", "true")`

**Issue 3**: CI/CD not triggering job
- **Cause**: Missing `DATABRICKS_JOB_ID` secret
- **Fix**: Add job ID to GitHub secrets

**Issue 4**: Git folder not syncing
- **Cause**: Haven't pulled latest changes
- **Fix**: Click "Pull" in Git folder

---

## 🎯 Best Practices

### Development

- ✅ Use version control (Git)
- ✅ Write parameterized notebooks
- ✅ Add error handling
- ✅ Document your code
- ✅ Test locally before pushing

### Production

- ✅ Set up automated jobs
- ✅ Configure monitoring & alerts
- ✅ Use CI/CD pipeline
- ✅ Implement data quality checks
- ✅ Create runbooks

---

## 📊 Expected Results

### Bronze Table

```
Records: 40
Columns: 12
- Original 9 columns
- ingested_at
- source_file
- ingestion_date
```

### Silver Table

```
Records: 40 (cleaned)
Columns: 16
- All Bronze columns
- total_amount
- sale_date, sale_month, sale_year
- transaction_size
- price_category
- is_valid
```

### Gold Tables

**gold_daily_sales**:
```
Records: 20 (unique dates)
Columns: 10
- total_transactions
- total_revenue
- avg_transaction_value
- total_items_sold
```

**gold_category_sales**:
```
Records: 5 (unique categories)
- total_transactions
- total_revenue
- avg_unit_price
```

**gold_city_sales**:
```
Records: 3 (unique cities)
- total_transactions
- total_revenue
- unique_customers
```

---

## 🎓 Learning Path

### Beginner
1. Read Burmese guide
2. Run notebooks manually
3. Understand data flow

### Intermediate
4. Modify transformations
5. Create visualizations
6. Set up jobs

### Advanced
7. Implement CI/CD
8. Add monitoring
9. Multi-environment setup

---

## � License

This project is for educational purposes.

---

## 👤 Author

**Learning Project** - Databricks Sales Pipeline

---

## 🙏 Acknowledgments

- Databricks documentation
- Apache Spark community
- Delta Lake contributors

---

## 📞 Support

For questions or issues:
1. Check troubleshooting section
2. Review Databricks documentation
3. Check GitHub Actions logs
4. Review job run logs

---

## 🎉 Status

| Component | Status |
|-----------|--------|
| Bronze Layer | ✅ Complete |
| Silver Layer | ✅ Complete |
| Gold Layer | ✅ Complete |
| Job Automation | ✅ Complete |
| CI/CD Pipeline | ✅ Complete |
| Documentation | ✅ Complete |
| Testing | ✅ Complete |

---

## 📈 CI/CD Status

![CI/CD Pipeline](https://github.com/yoloxsta/databricks-sales-pipeline/actions/workflows/ci-cd.yml/badge.svg)

---

**Built with ❤️ for learning Databricks**
