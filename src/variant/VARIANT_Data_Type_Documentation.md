# VARIANT Data Type Exploration in Databricks

## Overview

This comprehensive notebook demonstrates the capabilities and best practices for working with the VARIANT data type in Databricks. The VARIANT data type is a powerful feature that allows you to store semi-structured data (JSON, Avro, ORC, Parquet, or XML) in a single column while maintaining the ability to query and analyze the data efficiently.

## Table of Contents

1. [Introduction to VARIANT](#introduction-to-variant)
2. [Setup and Configuration](#setup-and-configuration)
3. [Data Generation and Storage](#data-generation-and-storage)
4. [Table Creation and Schema Design](#table-creation-and-schema-design)
5. [Data Loading Strategies](#data-loading-strategies)
6. [Querying VARIANT Data](#querying-variant-data)
7. [Type Casting and Conversion](#type-casting-and-conversion)
8. [VARIANT Functions and Operations](#variant-functions-and-operations)
9. [Filtering and Conditional Logic](#filtering-and-conditional-logic)
10. [Aggregations and Analytics](#aggregations-and-analytics)
11. [Window Functions](#window-functions)
12. [JOINs with VARIANT Data](#joins-with-variant-data)
13. [Advanced Use Cases](#advanced-use-cases)
14. [Performance Optimization](#performance-optimization)
15. [Best Practices](#best-practices)
16. [Troubleshooting](#troubleshooting)
17. [Cleanup and Maintenance](#cleanup-and-maintenance)

## Introduction to VARIANT

The VARIANT data type in Databricks is designed to handle semi-structured data efficiently. It provides:

- **Schema Flexibility**: Store data with varying structures without predefined schemas
- **Query Performance**: Optimized for JSON-like data access patterns
- **Type Safety**: Automatic type inference and safe casting
- **Storage Efficiency**: Compressed storage for semi-structured data

### Key Benefits

1. **Schema Evolution**: Handle data with evolving schemas seamlessly
2. **Unified Storage**: Store different data formats in a single column
3. **Flexible Querying**: Access nested fields using dot notation
4. **Performance**: Optimized for analytical workloads

## Setup and Configuration

### Prerequisites

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import os
```

### Configuration Parameters

```python
# Update these parameters to customize the notebook for your environment
CATALOG = "users"                    # Unity Catalog name
SCHEMA = "pavan_naidu"               # Schema name within the catalog
VOLUME = "raw_data"                  # Volume name for storing data
```

### Spark Session Setup

```python
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        return SparkSession.builder.getOrCreate()

spark = get_spark()
```

## Data Generation and Storage

### Simulating Schema Evolution

The notebook generates realistic user data that simulates schema evolution over time:

- **Phase 1 (30% of users)**: Basic user schema with core fields
- **Phase 2 (30% of users)**: Added social media fields
- **Phase 3 (40% of users)**: Added subscription and metrics fields

### Data Structure

```json
{
  "user_id": "uuid",
  "username": "string",
  "email": "string",
  "name": "string",
  "age": "integer",
  "created_at": "timestamp",
  "last_login": "timestamp",
  "profile": {
    "bio": "string",
    "occupation": "string",
    "company": "string",
    "interests": ["array"],
    "skills": ["array"]
  },
  "address": {
    "street": "string",
    "city": "string",
    "state": "string",
    "country": "string",
    "postal_code": "string",
    "coordinates": {
      "latitude": "float",
      "longitude": "float"
    }
  },
  "preferences": {
    "newsletter": "boolean",
    "notifications": {
      "email": "boolean",
      "sms": "boolean",
      "push": "boolean",
      "frequency": "string"
    },
    "privacy": {
      "profile_visible": "boolean",
      "show_email": "boolean",
      "show_location": "boolean"
    }
  }
}
```

## Table Creation and Schema Design

### Basic VARIANT Table

```sql
CREATE TABLE IF NOT EXISTS users_variant (
    user_id STRING,
    username STRING,
    email STRING,
    user_data VARIANT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Table storing user data in VARIANT column'
```

### Optimized Table with Extracted Fields

```sql
CREATE TABLE IF NOT EXISTS users_extracted (
    user_id STRING,
    username STRING,
    email STRING,
    name STRING,
    age INT,
    city STRING,
    country STRING,
    subscription_tier STRING,
    user_data VARIANT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Table with commonly accessed fields extracted from VARIANT'
```

## Data Loading Strategies

### Method 1: Using PARSE_JSON

```sql
INSERT INTO users_variant (user_id, username, email, user_data)
SELECT 
    get_json_object(value, '$.user_id') as user_id,
    get_json_object(value, '$.username') as username,
    get_json_object(value, '$.email') as email,
    PARSE_JSON(value) as user_data
FROM (
    SELECT value 
    FROM text.`/path/to/data.jsonl`
)
```

### Method 2: With Extracted Fields

```sql
INSERT INTO users_extracted 
SELECT 
    get_json_object(value, '$.user_id') as user_id,
    get_json_object(value, '$.username') as username,
    get_json_object(value, '$.email') as email,
    get_json_object(value, '$.name') as name,
    CAST(get_json_object(value, '$.age') AS INT) as age,
    get_json_object(value, '$.address.city') as city,
    get_json_object(value, '$.address.country') as country,
    get_json_object(value, '$.subscription.tier') as subscription_tier,
    PARSE_JSON(value) as user_data,
    CURRENT_TIMESTAMP() as ingestion_timestamp
FROM (
    SELECT value 
    FROM text.`/path/to/data.jsonl`
)
```

## Querying VARIANT Data

### Basic Field Access

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:age as age,
    user_data:profile:bio as bio,
    user_data:profile:occupation as occupation,
    user_data:address:city as city,
    user_data:address:coordinates:latitude as latitude,
    user_data:address:coordinates:longitude as longitude
FROM users_variant
LIMIT 5
```

### Array Element Access

```sql
SELECT 
    user_id,
    username,
    user_data:profile:interests[0] as first_interest,
    user_data:profile:interests[1] as second_interest,
    SIZE(CAST(user_data:profile:interests AS ARRAY<STRING>)) as total_interests,
    user_data:profile:skills as all_skills
FROM users_variant
WHERE user_data:profile:interests IS NOT NULL
LIMIT 5
```

### Exploding Arrays

```sql
SELECT 
    user_id,
    username,
    interest
FROM users_variant
LATERAL VIEW EXPLODE(CAST(user_data:profile:interests AS ARRAY<STRING>)) AS interest
LIMIT 10
```

## Type Casting and Conversion

### Basic Type Casting

```sql
SELECT 
    user_id,
    user_data:age as age_variant,
    CAST(user_data:age AS INT) as age_int,
    CAST(user_data:created_at AS TIMESTAMP) as created_timestamp,
    CAST(user_data:preferences:newsletter AS BOOLEAN) as newsletter_bool,
    CAST(user_data:metrics:login_count AS INT) as login_count_int
FROM users_variant
LIMIT 5
```

### Safe Casting with TRY_CAST

```sql
SELECT 
    user_id,
    TRY_CAST(user_data:age AS INT) as age,
    TRY_CAST(user_data:subscription:start_date AS DATE) as subscription_date,
    TRY_CAST(user_data:metrics:posts_created AS INT) as posts_count,
    TRY_CAST(user_data:phone AS STRING) as phone_number,
    TRY_CAST(user_data:last_login AS TIMESTAMP) as last_login_time
FROM users_variant
LIMIT 5
```

## VARIANT Functions and Operations

### VARIANT_GET Function

```sql
SELECT 
    user_id,
    VARIANT_GET(user_data, '$.name', 'STRING') as name,
    VARIANT_GET(user_data, '$.age', 'INT') as age,
    VARIANT_GET(user_data, '$.profile.interests', 'ARRAY<STRING>') as interests,
    VARIANT_GET(user_data, '$.non_existent_field', 'STRING') as missing_field
FROM users_variant
LIMIT 3
```

### JSON Conversion Functions

```sql
SELECT 
    user_id,
    TO_JSON(user_data) as json_string,
    LENGTH(TO_JSON(user_data)) as json_length,
    PARSE_JSON(TO_JSON(user_data:address)) as address_variant
FROM users_variant
LIMIT 2
```

### Schema Discovery

```sql
SELECT 
    SCHEMA_OF_VARIANT(user_data) as inferred_schema
FROM users_variant
GROUP BY SCHEMA_OF_VARIANT(user_data)
LIMIT 5
```

## Filtering and Conditional Logic

### Basic Filtering

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:age as age,
    user_data:address:country as country
FROM users_variant
WHERE CAST(user_data:age AS INT) > 30
    AND CAST(user_data:preferences:newsletter AS BOOLEAN) = true
    AND user_data:address:country IS NOT NULL
LIMIT 5
```

### Complex Filtering

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:age as age,
    CAST(user_data:subscription:tier AS STRING) as subscription_tier,
    user_data:metrics:login_count as login_count
FROM users_variant
WHERE user_data:subscription:tier IS NOT NULL
    AND user_data:metrics:login_count IS NOT NULL
    AND CAST(user_data:age AS INT) BETWEEN 25 AND 60
    AND CAST(user_data:metrics:login_count AS INT) > 10
ORDER BY CAST(user_data:metrics:login_count AS INT) DESC
LIMIT 5
```

### CASE Statements

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:age as age,
    CAST(user_data:subscription:tier AS STRING) as subscription_tier,
    CASE 
        WHEN CAST(user_data:age AS INT) < 25 THEN 'Young'
        WHEN CAST(user_data:age AS INT) BETWEEN 25 AND 50 THEN 'Adult'
        WHEN CAST(user_data:age AS INT) > 50 THEN 'Senior'
        ELSE 'Unknown'
    END as age_group,
    CASE 
        WHEN CAST(user_data:subscription:tier AS STRING) = 'free' THEN 'Free User'
        WHEN CAST(user_data:subscription:tier AS STRING) = 'basic' THEN 'Basic User'
        WHEN CAST(user_data:subscription:tier AS STRING) = 'premium' THEN 'Premium User'
        WHEN CAST(user_data:subscription:tier AS STRING) = 'enterprise' THEN 'Enterprise User'
        ELSE 'No Subscription'
    END as user_category
FROM users_variant
LIMIT 10
```

## Aggregations and Analytics

### Basic Aggregations

```sql
SELECT 
    COUNT(*) as user_count,
    AVG(CAST(user_data:age AS INT)) as avg_age,
    MIN(CAST(user_data:age AS INT)) as min_age,
    MAX(CAST(user_data:age AS INT)) as max_age
FROM users_variant
```

### Grouped Aggregations

```sql
SELECT 
    CAST(user_data:subscription:tier AS STRING) as subscription_tier,
    COUNT(*) as user_count,
    AVG(CAST(user_data:age AS INT)) as avg_age,
    AVG(CAST(user_data:metrics:login_count AS INT)) as avg_login_count,
    AVG(CAST(user_data:metrics:posts_created AS INT)) as avg_posts
FROM users_variant
WHERE user_data:subscription:tier IS NOT NULL
GROUP BY CAST(user_data:subscription:tier AS STRING)
ORDER BY user_count DESC
```

### Array Aggregations

```sql
SELECT 
    AVG(SIZE(CAST(user_data:profile:interests AS ARRAY<STRING>))) as avg_interests_per_user,
    MAX(SIZE(CAST(user_data:profile:interests AS ARRAY<STRING>))) as max_interests,
    MIN(SIZE(CAST(user_data:profile:interests AS ARRAY<STRING>))) as min_interests
FROM users_variant
WHERE user_data:profile:interests IS NOT NULL
```

## Window Functions

### Ranking and Partitioning

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:age as age,
    CAST(user_data:address:country AS STRING) as country,
    CAST(user_data:metrics:login_count AS INT) as login_count,
    RANK() OVER (PARTITION BY CAST(user_data:address:country AS STRING) ORDER BY CAST(user_data:age AS INT) DESC) as age_rank_in_country,
    DENSE_RANK() OVER (ORDER BY CAST(user_data:metrics:login_count AS INT) DESC) as login_rank,
    AVG(CAST(user_data:age AS INT)) OVER (PARTITION BY CAST(user_data:address:country AS STRING)) as country_avg_age
FROM users_variant
WHERE user_data:metrics:login_count IS NOT NULL
    AND user_data:address:country IS NOT NULL
ORDER BY country, age_rank_in_country
LIMIT 15
```

### Running Totals

```sql
WITH user_activity AS (
    SELECT 
        user_id,
        user_data:name as name,
        CAST(user_data:created_at AS TIMESTAMP) as created_time,
        CAST(user_data:metrics:login_count AS INT) as login_count,
        CAST(user_data:metrics:posts_created AS INT) as posts_created
    FROM users_variant
    WHERE user_data:metrics:login_count IS NOT NULL
        AND user_data:created_at IS NOT NULL
)
SELECT 
    user_id,
    name,
    created_time,
    login_count,
    posts_created,
    AVG(login_count) OVER (
        ORDER BY created_time 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_logins,
    MAX(login_count) OVER (
        ORDER BY created_time
    ) as max_logins_so_far
FROM user_activity
ORDER BY created_time
LIMIT 10
```

## JOINs with VARIANT Data

### Self-Join

```sql
SELECT 
    u1.user_id as user1_id,
    u1.user_data:name as user1_name,
    CAST(u1.user_data:address:city AS STRING) as user1_city,
    u2.user_id as user2_id,
    u2.user_data:name as user2_name,
    CAST(u2.user_data:address:city AS STRING) as user2_city
FROM users_variant u1
JOIN users_variant u2
    ON CAST(u1.user_data:address:city AS STRING) = CAST(u2.user_data:address:city AS STRING)
    AND u1.user_id < u2.user_id
WHERE u1.user_data:address:city IS NOT NULL
LIMIT 10
```

### Performance-Optimized Join

```sql
WITH user_analysis AS (
    SELECT 
        v.user_id,
        v.user_data:name as name,
        v.user_data:age as age,
        CAST(v.user_data:address:city AS STRING) as city,
        CAST(v.user_data:subscription:tier AS STRING) as subscription_tier,
        e.name as extracted_name,
        e.age as extracted_age,
        e.city as extracted_city
    FROM users_variant v
    LEFT JOIN users_extracted e
        ON v.user_id = e.user_id
    WHERE v.user_data:subscription:tier IS NOT NULL
)
SELECT * FROM user_analysis
WHERE name IS NOT NULL
LIMIT 5
```

## Advanced Use Cases

### Schema Evolution Handling

The VARIANT data type excels at handling schema evolution. The notebook demonstrates how different schema versions can coexist:

```sql
SELECT 
    user_id,
    user_data:name as name,
    user_data:email as email,
    user_data:phone as phone,  -- Optional field
    user_data:social_media:twitter as twitter,  -- Added in v2
    user_data:subscription:tier as subscription_tier,  -- Added in v3
    user_data:metrics:login_count as login_count  -- Added in v3
FROM users_variant
WHERE user_id IN (
    (SELECT user_id FROM users_variant LIMIT 1 OFFSET 100)
    UNION ALL
    (SELECT user_id FROM users_variant LIMIT 1 OFFSET 500)
    UNION ALL
    (SELECT user_id FROM users_variant LIMIT 1 OFFSET 900)
)
```

### Pivoting VARIANT Data

```sql
WITH subscription_counts AS (
    SELECT 
        CAST(user_data:address:country AS STRING) as country,
        CAST(user_data:subscription:tier AS STRING) as subscription_tier,
        COUNT(*) as user_count
    FROM users_variant
    WHERE user_data:subscription:tier IS NOT NULL
        AND user_data:address:country IS NOT NULL
    GROUP BY country, subscription_tier
)
SELECT * FROM subscription_counts
PIVOT (
    SUM(user_count)
    FOR subscription_tier IN ('free', 'basic', 'premium', 'enterprise')
)
ORDER BY country
LIMIT 10
```

### Creating Structured Views

```sql
CREATE OR REPLACE TEMPORARY VIEW user_details AS
SELECT 
    user_id,
    CAST(user_data:name AS STRING) as name,
    CAST(user_data:email AS STRING) as email,
    CAST(user_data:age AS INT) as age,
    CAST(user_data:address:city AS STRING) as city,
    CAST(user_data:address:country AS STRING) as country,
    CAST(user_data:preferences:newsletter AS BOOLEAN) as newsletter_opt_in,
    CAST(user_data:created_at AS TIMESTAMP) as created_date,
    SIZE(CAST(user_data:profile:interests AS ARRAY<STRING>)) as interest_count
FROM users_variant
```

## Performance Optimization

### Extracted Columns Strategy

For frequently accessed fields, create extracted columns:

```sql
CREATE OR REPLACE TABLE variant_optimized
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
AS
SELECT 
    user_id,
    username,
    email,
    user_data,
    -- Extract frequently accessed fields as separate columns
    CAST(user_data:name AS STRING) as extracted_name,
    CAST(user_data:age AS INT) as extracted_age,
    CAST(user_data:created_at AS TIMESTAMP) as extracted_timestamp,
    ingestion_timestamp
FROM users_variant
```

### Efficient Filtering

```sql
SELECT 
    extracted_name,
    username,
    user_data:email as email
FROM variant_optimized
WHERE extracted_name LIKE 'J%'  -- Uses extracted column for efficient filtering
LIMIT 5
```

## Best Practices

### 1. Schema Design

- **Extract Common Fields**: Create separate columns for frequently accessed fields
- **Use Appropriate Data Types**: Cast VARIANT fields to appropriate types when possible
- **Plan for Schema Evolution**: Design tables to handle changing data structures

### 2. Query Optimization

- **Use Extracted Columns**: For filtering and joining on common fields
- **Avoid Deep Nesting**: Limit the depth of nested field access
- **Use TRY_CAST**: For safe type conversion with null handling

### 3. Data Loading

- **Batch Processing**: Load data in batches for better performance
- **Validate Data**: Check data quality before loading into VARIANT columns
- **Use Compression**: Enable appropriate compression for VARIANT data

### 4. Maintenance

- **Regular Cleanup**: Remove unused tables and views
- **Monitor Performance**: Track query performance on VARIANT columns
- **Update Statistics**: Keep table statistics current for optimal query planning

## Troubleshooting

### Common Issues

#### 1. Type Casting Errors

**Problem**: `CAST` fails on VARIANT fields with unexpected data types.

**Solution**: Use `TRY_CAST` for safe conversion:

```sql
-- Instead of:
CAST(user_data:age AS INT) as age

-- Use:
TRY_CAST(user_data:age AS INT) as age
```

#### 2. Null Value Handling

**Problem**: Null values in VARIANT fields cause query failures.

**Solution**: Add null checks:

```sql
SELECT 
    user_id,
    user_data:name as name
FROM users_variant
WHERE user_data:name IS NOT NULL
```

#### 3. Performance Issues

**Problem**: Slow queries on VARIANT columns.

**Solution**: 
- Extract frequently accessed fields
- Use appropriate indexes
- Consider partitioning strategies

#### 4. Schema Evolution Challenges

**Problem**: New fields not available in older records.

**Solution**: Use conditional logic:

```sql
SELECT 
    user_id,
    CASE 
        WHEN user_data:subscription:tier IS NOT NULL 
        THEN CAST(user_data:subscription:tier AS STRING)
        ELSE 'unknown'
    END as subscription_tier
FROM users_variant
```

### Debugging Tips

1. **Use SCHEMA_OF_VARIANT**: Discover the structure of VARIANT data
2. **Test with Sample Data**: Use small datasets for testing queries
3. **Monitor Query Plans**: Use `EXPLAIN` to understand query execution
4. **Validate Data Types**: Check data types before casting

## Cleanup and Maintenance

### Comprehensive Cleanup

```python
def cleanup():
    """Simple cleanup function to remove all created resources"""
    print("🧹 Cleaning up VARIANT exploration resources...")
    
    # Drop tables
    tables = ['users_variant', 'users_extracted', 'variant_optimized']
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"✅ Dropped table: {table}")
    
    # Drop views
    spark.sql("DROP VIEW IF EXISTS user_details")
    print("✅ Dropped view: user_details")
    
    # Clean volume files
    users_folder = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/users"
    try:
        dbutils.fs.rm(users_folder, recurse=True)
        print("✅ Cleaned volume files")
    except:
        print("⚠️  Volume files may not exist")
    
    # Reset to default catalog/schema
    spark.sql("USE CATALOG main")
    spark.sql("USE SCHEMA default")
    print("✅ Reset to main catalog, default schema")
    
    print("\n🎉 Cleanup complete!")
```

### Selective Cleanup

```python
def drop_tables_only():
    """Drop only the tables"""
    tables = ['users_variant', 'users_extracted', 'variant_optimized']
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"✅ Dropped {table}")

def clean_volume_only():
    """Clean only the volume files"""
    dbutils.fs.rm(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/users", recurse=True)
    print("✅ Cleaned volume files")
```

## Conclusion

The VARIANT data type in Databricks provides a powerful solution for handling semi-structured data with evolving schemas. By following the patterns and best practices outlined in this documentation, you can:

- Store and query complex JSON data efficiently
- Handle schema evolution gracefully
- Optimize performance for analytical workloads
- Maintain data quality and consistency

This notebook serves as a comprehensive reference for working with VARIANT data types in Databricks, covering everything from basic operations to advanced use cases and performance optimization strategies.

## Additional Resources

- [Databricks VARIANT Documentation](https://docs.databricks.com/sql/language-manual/data-types/variant-type.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
- [Spark SQL Functions Reference](https://spark.apache.org/docs/latest/api/sql/index.html)
