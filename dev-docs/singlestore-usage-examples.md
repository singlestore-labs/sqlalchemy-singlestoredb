# SingleStore Usage Examples and Best Practices

This guide provides examples and best practices for working with SingleStore databases, based on real-world usage patterns from SingleStore Spaces notebooks.

## Database Connection Patterns

### Using singlestoredb Python Driver

```python
import singlestoredb as s2

# Basic connection
connection = s2.connect(connection_url)

# Connection with cursor
conn = s2.connect()
cursor = conn.cursor()

# Connection with context manager
with s2.connect(connection_url) as conn:
    with conn.cursor() as cur:
        cur.execute('SHOW DATABASES')
        for row in cur:
            print(*row)
```

### Using SQLAlchemy

```python
import sqlalchemy as sa
from sqlalchemy import create_engine, text

# Basic engine creation
engine = sa.create_engine(connection_url)
connection = engine.connect()

# Connection pool with SSL
import requests

ca_cert_url = "https://portal.singlestore.com/static/ca/singlestore_bundle.pem"
ca_cert_path = "/tmp/singlestore_bundle.pem"

response = requests.get(ca_cert_url)
with open(ca_cert_path, "wb") as f:
    f.write(response.content)

sql_connection_string = connection_url.replace("singlestoredb", "mysql+pymysql")
engine = create_engine(
    f"{sql_connection_string}?ssl_ca={ca_cert_path}",
    pool_size=10,           # Maximum number of connections in the pool
    max_overflow=5,         # Allow up to 5 additional connections
    pool_timeout=30         # Wait up to 30 seconds for a connection
)

def execute_query(query: str):
    with engine.connect() as connection:
        return connection.execute(text(query))

def execute_transaction(transactional_query: str):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            result = connection.execute(text(transactional_query))
            transaction.commit()
            return result
        except Exception as e:
            transaction.rollback()
            raise e
```

### Using mysql.connector (MySQL Compatibility)

```python
import mysql.connector

# Create connection leveraging MySQL compatibility
cxn = mysql.connector.connect(
    user=username,
    password=password,
    host=host,
    database=database
)
```

## Database Management

### Creating and Managing Databases

```sql
-- Show available databases
SHOW DATABASES;

-- Create database (may be restricted on free tier)
CREATE DATABASE database_name;

-- Drop database (use with extreme caution)
DROP DATABASE database_name;

-- Use specific database
USE database_name;

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS my_database;
```

### Database Backup and Restore

```sql
-- Query backup history
SELECT * FROM information_schema.MV_BACKUP_HISTORY
WHERE STATUS = 'Success'
  AND DATABASE_NAME = 'my_database'
ORDER BY BACKUP_ID DESC;

-- Restore database from S3
RESTORE DATABASE employees AS employees_restored
  FROM S3 'train.memsql.com/employee'
  CONFIG '{"region":"us-east-1"}'
  CREDENTIALS '{}';
```

## Basic Query Patterns

### Simple Queries

```sql
-- Basic selection with ordering
SELECT id, name FROM employees ORDER BY id;

-- Filtering with WHERE clause
SELECT id, name FROM employees WHERE state = 'NY' ORDER BY id;

-- Count records
SELECT COUNT(*) FROM employees;

-- Group by with aggregation
SELECT state, COUNT(*)
FROM employees
GROUP BY state
ORDER BY state;
```

### Working with JSON Data

```sql
-- Create table with JSON column
CREATE TABLE json_posts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data JSON,
    SHARD KEY (id)
);

-- Insert JSON data
INSERT INTO json_posts (data)
VALUES ('{"title": "Post One", "tags": ["news", "events"]}');

-- Query JSON fields
SELECT JSON_EXTRACT_STRING(data, '$.title') as title
FROM json_posts;

-- Cast JSON fields (SingleStore :> operator)
SELECT _id :> JSON, _more :> JSON FROM customers LIMIT 10;
```

## Advanced Query Examples

### Joins and Aggregations

```sql
-- Inner join with count
SELECT m.name, COUNT(*) count
FROM employees m
JOIN employees e ON m.id = e.managerId
GROUP BY m.id
ORDER BY count DESC;

-- Multiple joins with aggregation
SELECT m.name, SUM(salary)
FROM employees m
JOIN employees e ON m.id = e.managerId
JOIN salaries s ON s.employeeId = e.id
GROUP BY m.id
ORDER BY SUM(salary) DESC;

-- Complex join with JSON data
SELECT
    p._more::$full_name AS NameOfPerson,
    p._more::$email AS Email,
    a.id,
    a.name AS ATMName,
    a.city,
    t.transaction_id,
    t.transaction_date,
    t.amount,
    t.transaction_type,
    t.description
FROM
    profile p
JOIN
    atm_locations a ON p._more::$account_id = a.id
LEFT JOIN
    transactions t ON p._more::$account_id = t.account_id
LIMIT 10;
```

### Subqueries

```sql
-- IN operator with subquery
SELECT name
FROM employees
WHERE id IN (SELECT managerId FROM employees)
ORDER BY name;
```

## Data Ingestion Patterns

### Using Pipelines

```sql
-- Create pipeline for S3 data
CREATE PIPELINE SalesData_Pipeline AS
LOAD DATA S3 's3://singlestoreloaddata/SalesData/*.csv'
CONFIG '{ "region": "ap-south-1" }'
INTO TABLE SalesData
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 lines;

-- Start all pipelines
START ALL PIPELINES;

-- Show pipeline status
SHOW PIPELINES;
```

### CDC with MongoDB

```sql
-- Create MongoDB data link
CREATE LINK source_listingsAndReviews AS MONGODB
CONFIG '{
    "mongodb.hosts":"host1:27017,host2:27017,host3:27017",
    "collection.include.list": "sample_airbnb.*",
    "mongodb.ssl.enabled":"true",
    "mongodb.authsource":"admin",
    "mongodb.members.auto.discover": "true"
}'
CREDENTIALS '{
    "mongodb.user":"username",
    "mongodb.password":"password"
}';

-- Show data links
SHOW LINKS;
```

## Vector Data Operations

```sql
-- Create table with vector column
CREATE TABLE embeddings_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    text_content TEXT,
    embedding VECTOR(768) NOT NULL,
    SHARD KEY (id)
);

-- Insert vector data
INSERT INTO embeddings_table (text_content, embedding)
VALUES ('Sample text', JSON_ARRAY_PACK('[0.1, 0.2, ...]'));

-- Vector similarity search (dot product)
SELECT text_content,
       DOT_PRODUCT(embedding, JSON_ARRAY_PACK('[query_vector]')) as similarity
FROM embeddings_table
ORDER BY similarity DESC
LIMIT 10;
```

## Performance Analysis

### Query Profiling

```sql
-- Profile a query to analyze execution plan
PROFILE SELECT e.first_name, e.last_name, d.dept_name
FROM employees e
INNER JOIN dept_emp de ON e.emp_no = de.emp_no
INNER JOIN departments d ON de.dept_no = d.dept_no
ORDER BY e.first_name, e.last_name
LIMIT 10;
```

### Performance Monitoring Queries

```sql
-- Check for datatype mismatches in query plans
SELECT m2.plan_id as PLAN_ID,
       m2.database_name as DATABASE_NAME,
       m2.cmp_exp as CMP_EXP,
       m2.left_type as LEFT_TYPE,
       m2.right_type as RIGHT_TYPE,
       p2.tbl_row_counts as TBL_ROW_COUNTS,
       p2.query_text AS SQL_QUERY
FROM mismatche_cmp_2 m2,
     plan_tbl_row_counts p2
WHERE m2.plan_id = p2.plan_id
  AND m2.database_name = p2.database_name;
```

## Python Integration Patterns

### Query Execution with Error Handling

```python
import logging

def execute_query(dbcon, query_txt):
    """
    Execute a SQL query on the specified database connection.

    Parameters
    ----------
    dbcon : connection
        The database connection object.
    query_txt : str
        The SQL query to execute.

    Returns
    -------
    list
        A list of rows returned by the query.
    """
    try:
        with dbcon.cursor() as cur:
            cur.execute(query_txt)
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        raise Exception('Failed to execute query')
```

### Using Pandas for Data Processing

```python
import pandas as pd
from sqlalchemy import create_engine

# Read data into DataFrame
def execute_query_to_dataframe(query: str):
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection)

# Example usage
query = """
    SELECT
        id,
        name,
        population,
        shape :> TEXT AS polygon,
        centroid :> TEXT AS point
    FROM
        neighborhoods
"""

df = pd.read_sql(query, db_connection)
```

### Batch Processing

```python
def process_records_in_batches(conn, query, batch_size=1000):
    """
    Process database records in batches to handle large datasets.
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        while True:
            records = cursor.fetchmany(batch_size)
            if not records:
                break

            # Process batch
            for record in records:
                # Process individual record
                process_record(record)
```

## Table Creation Patterns

### Basic Table Creation

```sql
-- Simple table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    country VARCHAR(100)
);

-- Table with sharding
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    amount DECIMAL(10,2),
    product VARCHAR(255),
    SHARD KEY (customer_id)
);
```

### Advanced Table Features

```sql
-- Table with JSON and vector columns
CREATE TABLE content_embeddings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    content_text TEXT,
    metadata JSON,
    embedding VECTOR(1536),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    SHARD KEY (id),
    KEY (created_at)
);
```

## Best Practices

### Connection Management

1. **Use Connection Pooling**: For applications with multiple concurrent queries
2. **Handle SSL Certificates**: Download and configure CA certificates for secure connections
3. **Implement Retry Logic**: Handle transient connection failures
4. **Close Connections**: Always close connections when done

### Query Optimization

1. **Use Appropriate Indexes**: Create indexes on frequently queried columns
2. **Leverage Sharding**: Use SHARD KEY for distributed tables
3. **Profile Queries**: Use PROFILE command to analyze query execution
4. **Batch Operations**: Process large datasets in batches

### Data Types

1. **JSON Operations**: Use `:>` operator for casting JSON fields
2. **Vector Data**: Use VECTOR type for AI/ML workloads
3. **Date/Time**: Use appropriate temporal data types
4. **Text vs VARCHAR**: Choose based on expected data size

### Error Handling

```python
# Comprehensive error handling example
def safe_execute_query(connection, query, params=None):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()
    except mysql.connector.Error as e:
        logging.error(f"MySQL Error {e.errno}: {e.msg}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
```

### Security Considerations

1. **Use Parameterized Queries**: Prevent SQL injection
2. **Secure Credentials**: Store credentials securely
3. **SSL/TLS**: Use encrypted connections
4. **Least Privilege**: Grant minimal required permissions

This guide provides a foundation for working with SingleStore databases effectively, covering both basic operations and advanced patterns for production use.
