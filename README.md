# Databricks Sales Pipeline Project

This project demonstrates a complete ETL pipeline using Databricks with Medallion Architecture (Bronze → Silver → Gold).

## 📁 Project Structure

```
databricks-sales-pipeline/
├── databricks.yml              # Databricks Asset Bundle configuration
├── README.md                   # This file
├── notebooks/
│   ├── 01_bronze.py           # Raw data ingestion
│   ├── 02_silver.py           # Data cleaning & transformation
│   └── 03_gold.py             # Business aggregations
└── config/
    └── dev.yml                # Development environment config
```

## 🏗️ Architecture

### Medallion Architecture

```
CSV File (Volume)
      │
      ▼
  Bronze Layer (Raw + Metadata)
      │
      ▼
  Silver Layer (Clean + Transformed)
      │
      ▼
  Gold Layer (Business Aggregations)
      │
      ▼
  Dashboard / BI
```

### Data Flow

| Layer | Table | Purpose |
|-------|-------|---------|
| Bronze | `sales_bronze` | Raw data with ingestion metadata |
| Silver | `sales_silver` | Cleaned data with derived columns |
| Gold | `gold_daily_sales` | Daily aggregated metrics |
| Gold | `gold_category_sales` | Category performance metrics |
| Gold | `gold_city_sales` | City-wise sales metrics |

## 🚀 Quick Start

### Prerequisites

- Databricks workspace (Free Edition or Azure Databricks)
- Databricks CLI installed
- Git repository access

### Setup

1. **Clone repository to Databricks:**
   - In Databricks, go to **Workspace** → **Create Git Folder**
   - Enter your GitHub repository URL
   - Click **Create**

2. **Configure Databricks CLI:**
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

3. **Deploy pipeline:**
   ```bash
   databricks bundle deploy --target dev
   ```

4. **Run pipeline:**
   ```bash
   databricks bundle run sales_etl_job --target dev
   ```

### Manual Execution

Run notebooks in order:
1. `notebooks/01_bronze.py`
2. `notebooks/02_silver.py`
3. `notebooks/03_gold.py`

## 📊 Expected Output

### Bronze Layer
- Adds `ingested_at`, `source_file`, `ingestion_date` columns
- Stores raw data in `workspace.default.sales_bronze`

### Silver Layer
- Removes invalid records
- Calculates `total_amount`, `sale_date`, `transaction_size`
- Adds categorizations (transaction size, price category)
- Stores in `workspace.default.sales_silver`

### Gold Layer
- **Daily Sales**: Daily aggregated metrics
- **Category Performance**: Revenue by product category
- **City Sales**: Geographic sales breakdown

## ⚙️ Configuration

### Parameters

Each notebook accepts parameters:
- `catalog`: Unity Catalog name (default: `workspace`)
- `schema`: Schema name (default: `default`)
- `source_file`: Source CSV filename (default: `sales_data.csv`)

### Schedule

Dev environment: Every 4 hours
Prod environment: Daily at 6 AM

## 📈 Monitoring

- Check job status in **Workflows** → **Jobs**
- View run history and logs
- Set up email notifications for failures

## 🔧 Development

### Adding New Features

1. Create feature branch
2. Modify notebooks
3. Test in dev environment
4. Create pull request
5. Deploy to production

### Testing

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Run test job
databricks bundle run sales_etl_job --target dev
```

## 📝 Notes

- This is a learning/demo project
- For production, add data quality checks, error handling, and monitoring
- Consider using dbt for transformations in production

## 📚 References

- [Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
- [Medallion Architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
- [Databricks Asset Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)

## 👤 Author

Your Name - Databricks Learning Project

## 📄 License

This project is for educational purposes.
