# CI/CD Setup Guide for Databricks Pipeline

This guide explains how to set up Continuous Integration and Continuous Deployment (CI/CD) for your Databricks pipeline using GitHub Actions.

## 🎯 What You'll Learn

- How to automatically test your notebooks when you push changes
- How to automatically deploy to Databricks
- How to set up GitHub secrets for secure authentication
- How to configure different environments (dev, staging, prod)

---

## 📋 Prerequisites

1. ✅ GitHub repository (you have this)
2. ✅ Databricks workspace (you have this)
3. ⬜ Databricks workspace URL
4. ⬜ Databricks personal access token

---

## 🔐 Step 1: Get Databricks Credentials

### Get Your Workspace URL

1. Go to your Databricks workspace
2. Copy the URL from your browser address bar
   - Example: `https://adb-123456789.1.azuredatabricks.net`
   - Or Free Edition: `https://your-workname.cloud.databricks.com`

### Generate Personal Access Token

1. In Databricks, click your username (top right)
2. Go to **Settings** → **Developer**
3. Click **Manage** next to **Access Tokens**
4. Click **Generate new token**
5. Name: `GitHub Actions CI/CD`
6. Lifetime: Leave blank (or set expiry)
7. Click **Generate**
8. **Copy the token** (you won't see it again!)

---

## 🔑 Step 2: Add GitHub Secrets

### Go to Your Repository Settings

1. Go to: `https://github.com/yoloxsta/databricks-sales-pipeline`
2. Click **Settings** tab
3. In left sidebar, click **Secrets and variables** → **Actions**
4. Click **New repository secret**

### Add DATABRICKS_HOST

- **Name:** `DATABRICKS_HOST`
- **Secret:** Your workspace URL (e.g., `https://adb-123456789.1.azuredatabricks.net`)
- Click **Add secret**

### Add DATABRICKS_TOKEN

- **Name:** `DATABRICKS_TOKEN`
- **Secret:** Your personal access token
- Click **Add secret**

---

## 🚀 Step 3: Push CI/CD Workflow

The `.github/workflows/ci-cd.yml` file has been created. Push it to GitHub:

```bash
cd d:\2026\databrick\databricks-sales-pipeline

git add .

git commit -m "Add CI/CD pipeline with GitHub Actions"

git push
```

---

## ✅ Step 4: Verify CI/CD is Working

### Check GitHub Actions

1. Go to your repository on GitHub
2. Click **Actions** tab
3. You should see a workflow running
4. Click on it to see details:
   - ✅ Validate job: Checks notebook syntax
   - ✅ Deploy job: Syncs to Databricks (if on main branch)

### What Happens Automatically

```
You push to GitHub
    ↓
GitHub Actions starts
    ↓
Job 1: Validate
    ├── Check Python syntax in notebooks
    ├── Validate bundle configuration
    └── Run unit tests
    ↓
Job 2: Deploy (only on main branch)
    ├── Configure Databricks CLI
    ├── Sync notebooks to Git folder
    └── Trigger job run (optional)
    ↓
Job 3: Notify
    └── Report success/failure
```

---

## 🧪 Step 5: Run Tests Locally (Optional)

Before pushing, you can run tests locally:

```bash
# Install pytest
pip install pytest

# Run tests
pytest tests/test_notebooks.py -v

# Or run the test file directly
python tests/test_notebooks.py
```

---

## 🌍 Step 6: Multi-Environment Deployment (Advanced)

For dev, staging, and production environments:

### Create Environment Files

Create `environments/dev.yml`:

```yaml
# Development environment
workspace:
  host: https://dev-workspace.cloud.databricks.com
```

Create `environments/prod.yml`:

```yaml
# Production environment
workspace:
  host: https://prod-workspace.cloud.databricks.com
```

### Update GitHub Workflow

Add to `.github/workflows/ci-cd.yml`:

```yaml
deploy-dev:
  needs: validate
  runs-on: ubuntu-latest
  if: github.ref == 'refs/heads/develop'
  steps:
    - name: Deploy to Dev
      run: databricks bundle deploy --target dev

deploy-prod:
  needs: validate
  runs-on: ubuntu-latest
  if: github.ref == 'refs/heads/main'
  steps:
    - name: Deploy to Prod
      run: databricks bundle deploy --target prod
```

---

## 📊 CI/CD Best Practices

### 1. Branch Protection Rules

In GitHub repository settings:

- Go to **Settings** → **Branches**
- Add rule for `main` branch:
  - ✅ Require pull request before merging
  - ✅ Require status checks to pass
  - ✅ Require branches to be up to date

### 2. Pull Request Workflow

```
Create feature branch
    ↓
Make changes & push
    ↓
Create Pull Request
    ↓
GitHub Actions runs tests
    ↓
Tests pass ✅
    ↓
Code review & approve
    ↓
Merge to main
    ↓
Auto-deploy to production
```

### 3. Add Unit Tests

Create more tests in `tests/` folder:

```python
def test_data_quality():
    # Test data quality checks
    assert True

def test_schema_validation():
    # Test schema validation
    assert True
```

### 4. Add Integration Tests

Test the actual pipeline:

```python
def test_bronze_to_silver_flow():
    # Test that Bronze data flows to Silver
    pass

def test_silver_to_gold_flow():
    # Test that Silver data flows to Gold
    pass
```

---

## 🔧 Troubleshooting

### GitHub Actions Failing

**Error: "DATABRICKS_HOST not found"**
- Make sure you added the secret in GitHub
- Check the secret name matches exactly (case-sensitive)

**Error: "Authentication failed"**
- Verify your Databricks token is correct
- Check if token has expired
- Ensure token has proper permissions

**Error: "Syntax error in notebook"**
- Run tests locally to find the issue
- Fix syntax errors in notebooks
- Push the fix

### Tests Failing Locally

```bash
# Check Python syntax manually
python -m py_compile notebooks/01_bronze.py

# Run specific test
pytest tests/test_notebooks.py::test_bronze_notebook_syntax -v
```

---

## 📈 Next Steps

### Add More Automation

1. **Automatic job triggering**: Run Databricks job after deployment
2. **Slack/Teams notifications**: Alert team on deployment status
3. **Data validation**: Check data quality after job runs
4. **Performance tests**: Monitor job execution time
5. **Security scans**: Check for exposed secrets

### Monitor Your Pipeline

- Check GitHub Actions history
- Set up alerts for failed deployments
- Review Databricks job run history
- Monitor data quality in Gold tables

---

## 🎉 Summary

You now have:

✅ Automated testing on every push  
✅ Automated deployment to Databricks  
✅ Secure credential management  
✅ Local test execution  
✅ Multi-environment support (optional)  

Your pipeline is production-ready!

---

## 📚 Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [CI/CD Best Practices](https://docs.databricks.com/dev-tools/ci-cd/index.html)

---

## ❓ Questions?

If you encounter issues:

1. Check GitHub Actions logs for detailed error messages
2. Run tests locally to identify issues
3. Verify all secrets are configured correctly
4. Check Databricks job logs for runtime errors
