import os
from dabs import main


def load_env():
    """Load environment variables from .databricks.env file."""
    env_file = '.databricks/.databricks.env'
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value


def test_get_spark():
    """Test that we can create a Spark session."""
    load_env()
    spark = main.get_spark()
    assert spark is not None
    print(f"Spark session created: {spark}")


def test_get_taxis():
    """Test the get_taxis function with real Spark session."""
    load_env()
    spark = main.get_spark()
    taxis = main.get_taxis(spark)
    
    # Verify we got a DataFrame
    assert taxis is not None
    
    # Verify it has data (count > 5 as in original test)
    count = taxis.count()
    print(f"Taxi data count: {count}")
    assert count > 5
    
    # Show some sample data
    print("Sample taxi data:")
    taxis.show(5)