"""Example unit test to demonstrate GitHub Actions workflow."""

import pytest
from unittest.mock import Mock, patch


def test_example_passing():
    """Example test that passes."""
    assert 1 + 1 == 2


def test_example_with_mock():
    """Example test using mocks."""
    mock_obj = Mock()
    mock_obj.method.return_value = 42
    
    result = mock_obj.method()
    assert result == 42
    mock_obj.method.assert_called_once()


@pytest.mark.parametrize("input_val,expected", [
    (1, 2),
    (2, 4),
    (3, 6),
])
def test_parametrized_example(input_val, expected):
    """Example parametrized test."""
    assert input_val * 2 == expected


def test_spark_context_availability():
    """Test that we can import PySpark in Databricks environment."""
    try:
        from pyspark.sql import SparkSession
        # This will work in Databricks but might fail locally without spark
        spark = SparkSession.builder.appName("test").getOrCreate()
        assert spark is not None
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not available in this environment")
