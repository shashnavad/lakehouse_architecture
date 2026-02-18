# Testing Guide

This directory contains pytest test suites for the Lakehouse Architecture project.

## Test Structure

- `conftest.py`: Pytest fixtures and configuration
- `test_bronze.py`: Tests for Bronze layer ingestion
- `test_silver_data_quality.py`: Tests for data quality checks
- `test_silver_transformations.py`: Tests for data transformations
- `test_silver_incremental.py`: Tests for incremental processing

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_bronze.py
pytest tests/test_silver_data_quality.py
```

### Run with Verbose Output

```bash
pytest -v
```

### Run with Coverage

```bash
pytest --cov=scripts --cov-report=html
```

### Run Specific Test

```bash
pytest tests/test_bronze.py::test_bronze_schema_enforcement
```

## Test Categories

Tests are organized by layer and functionality:

- **Bronze Layer**: Schema enforcement, metadata columns, Delta Lake writes
- **Silver Data Quality**: Null checks, duplicates, value ranges, referential integrity
- **Silver Transformations**: Data standardization, derived columns
- **Silver Incremental**: Incremental processing logic

## Prerequisites

- Python 3.8+
- PySpark installed
- Delta Lake libraries
- pytest and pytest-spark

Install test dependencies:

```bash
pip install pytest pytest-spark pytest-cov
```

## Test Fixtures

The `conftest.py` provides:

- `spark_session`: Spark session for testing
- `sample_transactions_df`: Sample transaction data
- `sample_customers_df`: Sample customer data
- `sample_products_df`: Sample product data
- `test_data_dir`: Temporary directory for test data

## Writing New Tests

When adding new tests:

1. Follow naming convention: `test_<feature>_<aspect>.py`
2. Use fixtures from `conftest.py` when possible
3. Clean up temporary files/directories
4. Add docstrings explaining what is being tested
5. Use descriptive test names

Example:

```python
def test_feature_behavior(spark_session, sample_transactions_df):
    """Test that feature behaves correctly"""
    # Arrange
    df = sample_transactions_df
    
    # Act
    result = process_data(df)
    
    # Assert
    assert result.count() > 0
```

