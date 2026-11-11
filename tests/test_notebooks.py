"""
Unit tests for Databricks notebooks
Run locally with: pytest tests/
"""

import ast
import os


def test_bronze_notebook_syntax():
    """Test that Bronze notebook has valid Python syntax"""
    with open('notebooks/01_bronze.py', 'r') as f:
        code = f.read()
    
    # Try to parse the code
    try:
        ast.parse(code)
        assert True, "Bronze notebook syntax is valid"
    except SyntaxError as e:
        assert False, f"Syntax error in Bronze notebook: {e}"


def test_silver_notebook_syntax():
    """Test that Silver notebook has valid Python syntax"""
    with open('notebooks/02_silver.py', 'r') as f:
        code = f.read()
    
    try:
        ast.parse(code)
        assert True, "Silver notebook syntax is valid"
    except SyntaxError as e:
        assert False, f"Syntax error in Silver notebook: {e}"


def test_gold_notebook_syntax():
    """Test that Gold notebook has valid Python syntax"""
    with open('notebooks/03_gold.py', 'r') as f:
        code = f.read()
    
    try:
        ast.parse(code)
        assert True, "Gold notebook syntax is valid"
    except SyntaxError as e:
        assert False, f"Syntax error in Gold notebook: {e}"


def test_bronze_has_required_imports():
    """Test that Bronze notebook has required imports"""
    with open('notebooks/01_bronze.py', 'r') as f:
        code = f.read()
    
    assert 'from pyspark.sql import functions as F' in code or \
           'import pyspark.sql.functions as F' in code, \
           "Bronze notebook missing Spark imports"


def test_bronze_has_parameters():
    """Test that Bronze notebook has parameter widgets"""
    with open('notebooks/01_bronze.py', 'r') as f:
        code = f.read()
    
    assert 'dbutils.widgets' in code, "Bronze notebook missing parameter widgets"
    assert 'catalog' in code, "Bronze notebook missing 'catalog' parameter"
    assert 'schema' in code, "Bronze notebook missing 'schema' parameter"


def test_silver_reads_bronze():
    """Test that Silver notebook reads from Bronze table"""
    with open('notebooks/02_silver.py', 'r') as f:
        code = f.read()
    
    assert 'sales_bronze' in code, "Silver notebook doesn't read from Bronze table"
    assert 'spark.read.table' in code or 'spark.table' in code, \
           "Silver notebook doesn't use Spark read"


def test_gold_reads_silver():
    """Test that Gold notebook reads from Silver table"""
    with open('notebooks/03_gold.py', 'r') as f:
        code = f.read()
    
    assert 'sales_silver' in code, "Gold notebook doesn't read from Silver table"


def test_gold_creates_aggregations():
    """Test that Gold notebook creates aggregation tables"""
    with open('notebooks/03_gold.py', 'r') as f:
        code = f.read()
    
    assert 'gold_daily_sales' in code, "Gold notebook missing daily sales aggregation"
    assert 'gold_category_sales' in code or 'gold_city_sales' in code, \
           "Gold notebook missing category/city aggregations"


def test_all_notebooks_have_validation():
    """Test that all notebooks have validation steps"""
    notebooks = [
        'notebooks/01_bronze.py',
        'notebooks/02_silver.py',
        'notebooks/03_gold.py'
    ]
    
    for notebook in notebooks:
        with open(notebook, 'r') as f:
            code = f.read()
        
        # Check for common validation patterns
        has_validation = any([
            'assert' in code,
            'print("✓' in code,
            'count()' in code,
            '.limit(' in code
        ])
        
        assert has_validation, f"{notebook} doesn't have validation steps"


if __name__ == '__main__':
    # Run tests manually
    print("Running notebook tests...")
    test_bronze_notebook_syntax()
    print("✓ Bronze syntax OK")
    
    test_silver_notebook_syntax()
    print("✓ Silver syntax OK")
    
    test_gold_notebook_syntax()
    print("✓ Gold syntax OK")
    
    test_bronze_has_required_imports()
    print("✓ Bronze has required imports")
    
    test_bronze_has_parameters()
    print("✓ Bronze has parameters")
    
    test_silver_reads_bronze()
    print("✓ Silver reads Bronze")
    
    test_gold_reads_silver()
    print("✓ Gold reads Silver")
    
    test_gold_creates_aggregations()
    print("✓ Gold creates aggregations")
    
    test_all_notebooks_have_validation()
    print("✓ All notebooks have validation")
    
    print("\n✅ All tests passed!")
