# SingleStore Python SDK Reference Guide

This comprehensive guide covers the SingleStore Python SDK, connection methods, and usage patterns based on real-world examples from SingleStore Spaces notebooks.

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Connection Methods](#connection-methods)
3. [Database Operations](#database-operations)
4. [Data Types and Conversions](#data-types-and-conversions)
5. [Vector Operations](#vector-operations)
6. [Integration Patterns](#integration-patterns)
7. [Performance Optimization](#performance-optimization)
8. [Error Handling](#error-handling)
9. [Best Practices](#best-practices)

## Installation and Setup

### Required Dependencies

```bash
# Core SingleStore Python SDK
pip install singlestoredb

# Additional useful libraries
pip install sqlalchemy pandas numpy pymongo
```

### Environment Variables

```python
import os

# SingleStore connection URL
os.environ["SINGLESTOREDB_URL"] = "singlestoredb://user:password@host:port/database"

# For SSL connections
os.environ["SINGLESTOREDB_SSL_CA"] = "/path/to/ca-certificate.pem"
```

## Connection Methods

### 1. Using singlestoredb Package

#### Basic Connection
```python
import singlestoredb as s2

# Simple connection using environment variables
conn = s2.connect()
cursor = conn.cursor()

# Connection with explicit parameters
conn = s2.connect(
    host='hostname',
    port=3306,
    user='username',
    password='password',
    database='database_name'
)
```

#### Context Manager Pattern
```python
import singlestoredb as s2

# Recommended approach with automatic cleanup
with s2.connect() as conn:
    with conn.cursor() as cur:
        cur.execute('SHOW DATABASES')
        for row in cur:
            print(*row)
```

#### Connection with Workspace Endpoint
```python
import singlestoredb as s2

# Connect to specific workspace
workspace_endpoint = "workspace-abc123.svc.singlestore.com"
with s2.connect(f'admin:{password}@{workspace_endpoint}:3306') as conn:
    with conn.cursor() as cur:
        cur.execute('SHOW DATABASES')
        results = cur.fetchall()
```

### 2. Using SQLAlchemy Integration

#### Basic SQLAlchemy Engine
```python
import sqlalchemy as sa
import singlestoredb as s2

# Using singlestoredb wrapper
engine = s2.create_engine()
connection = engine.connect()

# Direct SQLAlchemy approach
engine = sa.create_engine(connection_url)
connection = engine.connect()
```

#### Connection Pool Configuration
```python
from sqlalchemy import create_engine, text
import requests

# Download CA certificate for SSL
ca_cert_url = "https://portal.singlestore.com/static/ca/singlestore_bundle.pem"
ca_cert_path = "/tmp/singlestore_bundle.pem"

response = requests.get(ca_cert_url)
with open(ca_cert_path, "wb") as f:
    f.write(response.content)

# Configure connection pool with SSL
sql_connection_string = connection_url.replace("singlestoredb", "mysql+pymysql")
engine = create_engine(
    f"{sql_connection_string}?ssl_ca={ca_cert_path}",
    pool_size=10,           # Maximum connections in pool
    max_overflow=5,         # Additional overflow connections
    pool_timeout=30,        # Wait timeout for connection
    pool_recycle=3600       # Recycle connections after 1 hour
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

### 3. Using MySQL Compatibility Layer

```python
import mysql.connector

# Leverage MySQL compatibility
cxn = mysql.connector.connect(
    user=username,
    password=password,
    host=host,
    port=3306,
    database=database,
    ssl_disabled=False
)
```

### 4. Using Ibis Integration

```python
import ibis

# Connect using Ibis for DataFrame-like operations
conn = ibis.singlestoredb.connect()

# Create table from DataFrame
phones_table = conn.create_table('phones', phones_df, overwrite=True)

# Query using Ibis expressions
result = phones_table.head(5)
```

## Database Operations

### Basic Query Execution

```python
import singlestoredb as s2

conn = s2.connect()
cursor = conn.cursor()

# Simple SELECT
cursor.execute('SELECT * FROM users LIMIT 10')
results = cursor.fetchall()

# Parameterized query
cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
user = cursor.fetchone()

# Batch operations
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
cursor.executemany('INSERT INTO users (id, name) VALUES (%s, %s)', data)
conn.commit()
```

### Using pandas Integration

```python
import pandas as pd
import singlestoredb as s2

# Read SQL into DataFrame
engine = s2.create_engine()
df = pd.read_sql('SELECT * FROM sales_data', engine)

# Write DataFrame to table
df.to_sql('processed_data', con=engine, if_exists='append', index=False)
```

### Table Management

```python
# Create table with SHARD KEY
cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        amount DECIMAL(10,2),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        SHARD KEY (customer_id)
    )
''')

# Drop table
cursor.execute('DROP TABLE IF EXISTS temp_table')

# Show table structure
cursor.execute('DESCRIBE orders')
schema = cursor.fetchall()
```

## Data Types and Conversions

### JSON Data Handling

```python
import json

# Insert JSON data
json_data = {"name": "John", "age": 30, "city": "New York"}
cursor.execute(
    "INSERT INTO users (profile) VALUES (%s)",
    (json.dumps(json_data),)
)

# Query JSON fields using SingleStore's :> operator
cursor.execute("SELECT profile :> '$.name' as name FROM users")
names = cursor.fetchall()

# Extract JSON arrays
cursor.execute("SELECT JSON_EXTRACT_STRING(data, '$.tags') FROM posts")
```

### Vector Data Operations

```python
import struct
import numpy as np

# Prepare vector data for insertion
def prepare_vector_for_db(vector_list):
    """Convert Python list to binary format for VECTOR column"""
    fmt = f'<{len(vector_list)}f'  # Little-endian float format
    return struct.pack(fmt, *vector_list)

# Insert vector data
embedding = [0.1, 0.2, 0.3, 0.4]  # Example embedding
binary_vector = prepare_vector_for_db(embedding)

cursor.execute(
    "INSERT INTO embeddings (text_content, embedding) VALUES (%s, %s)",
    ("Sample text", binary_vector)
)

# Query with vector similarity
query_vector = prepare_vector_for_db([0.15, 0.25, 0.35, 0.45])
cursor.execute("""
    SELECT text_content,
           DOT_PRODUCT(embedding, %s) as similarity
    FROM embeddings
    ORDER BY similarity DESC
    LIMIT 10
""", (query_vector,))

# Convert binary back to Python list
def unpack_vector_from_db(binary_data, vector_length):
    """Convert binary format back to Python list"""
    fmt = f'<{vector_length}f'
    return list(struct.unpack(fmt, binary_data))
```

### Working with VECTOR Data Type

```python
# Create table with VECTOR column
cursor.execute('''
    CREATE TABLE embeddings_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        text_content TEXT,
        embedding VECTOR(768) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        SHARD KEY (id)
    )
''')

# Insert using JSON_ARRAY_PACK
embedding_list = [0.1, 0.2, 0.3, ...]  # 768-dimensional vector
cursor.execute(
    "INSERT INTO embeddings_table (text_content, embedding) VALUES (%s, JSON_ARRAY_PACK(%s))",
    ("Sample text", str(embedding_list))
)

# Vector similarity search
cursor.execute("""
    SELECT text_content,
           DOT_PRODUCT(embedding, JSON_ARRAY_PACK(%s)) as similarity
    FROM embeddings_table
    ORDER BY similarity DESC
    LIMIT 10
""", (str(query_vector),))
```

## Vector Operations

### Vector Similarity Functions

```python
# Dot product similarity
cursor.execute("""
    SELECT content, DOT_PRODUCT(embedding, %s) as similarity
    FROM documents
    ORDER BY similarity DESC
    LIMIT 5
""", (query_embedding,))

# Euclidean distance
cursor.execute("""
    SELECT content, EUCLIDEAN_DISTANCE(embedding, %s) as distance
    FROM documents
    ORDER BY distance ASC
    LIMIT 5
""", (query_embedding,))

# Cosine similarity (normalized dot product)
cursor.execute("""
    SELECT content,
           DOT_PRODUCT(embedding, %s) /
           (VECTOR_LENGTH(embedding) * VECTOR_LENGTH(%s)) as cosine_sim
    FROM documents
    ORDER BY cosine_sim DESC
    LIMIT 5
""", (query_embedding, query_embedding))
```

### Batch Vector Operations

```python
import pandas as pd

# Prepare multiple vectors for batch insertion
vectors_df = pd.DataFrame({
    'content': ['Text 1', 'Text 2', 'Text 3'],
    'embedding': [vector1, vector2, vector3]
})

# Convert vectors to binary format
vectors_df['embedding_binary'] = vectors_df['embedding'].apply(
    lambda x: struct.pack(f'<{len(x)}f', *x)
)

# Batch insert using pandas
vectors_df[['content', 'embedding_binary']].to_sql(
    'embeddings',
    con=engine,
    if_exists='append',
    index=False
)
```

## Integration Patterns

### With Langchain

```python
from langchain_community.vectorstores import SingleStoreDB
from langchain_openai import OpenAIEmbeddings

# Initialize embeddings
embeddings = OpenAIEmbeddings()

# Set up SingleStoreDB vector store
os.environ["SINGLESTOREDB_URL"] = connection_url
vectorstore = SingleStoreDB.from_documents(
    documents=docs,
    embedding=embeddings,
    table_name="document_embeddings"
)

# Similarity search
results = vectorstore.similarity_search(query, k=5)
```

### With MongoDB API (Kai)

```python
import pymongo

# Connect using MongoDB-compatible API
client = pymongo.MongoClient(connection_url_kai)
database = client['your_database']
collection = database['your_collection']

# Standard MongoDB operations
doc = {"name": "John", "age": 30}
result = collection.insert_one(doc)

# Query documents
for doc in collection.find({"age": {"$gte": 18}}):
    print(doc)
```

### FastAPI Integration

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from singlestoredb import connect
from concurrent.futures import ThreadPoolExecutor
import asyncio

app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    limit: int = 10

# Thread pool for database operations
executor = ThreadPoolExecutor()

def run_in_thread(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(executor, fn, *args)

@app.post("/search")
async def search_documents(request: QueryRequest):
    def execute_search():
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT content FROM documents WHERE MATCH(content) AGAINST(%s) LIMIT %s",
                    (request.query, request.limit)
                )
                return cursor.fetchall()

    try:
        results = await run_in_thread(execute_search)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

## Performance Optimization

### Connection Pooling

```python
from sqlalchemy.pool import QueuePool

# Optimize connection pool settings
engine = create_engine(
    connection_url,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,        # Validate connections
    pool_recycle=3600,         # Recycle after 1 hour
)
```

### Batch Processing

```python
def process_records_in_batches(conn, query, batch_size=1000):
    """Process database records in batches to handle large datasets."""
    with conn.cursor() as cursor:
        cursor.execute(query)
        while True:
            records = cursor.fetchmany(batch_size)
            if not records:
                break

            # Process batch
            for record in records:
                process_record(record)
```

### Query Optimization

```python
# Use PROFILE to analyze query performance
cursor.execute("""
    PROFILE
    SELECT e.name, d.dept_name
    FROM employees e
    INNER JOIN departments d ON e.dept_id = d.id
    ORDER BY e.name
    LIMIT 100
""")

# Results show execution plan and timing
profile_results = cursor.fetchall()
```

## Error Handling

### Comprehensive Error Handling

```python
import mysql.connector
import logging

def safe_execute_query(connection, query, params=None):
    """Execute query with comprehensive error handling."""
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

### Connection Retry Logic

```python
import time
from functools import wraps

def retry_on_connection_error(max_retries=3, delay=1):
    """Decorator to retry database operations on connection errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (mysql.connector.Error, ConnectionError) as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Connection error on attempt {attempt + 1}: {e}")
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator

@retry_on_connection_error()
def execute_with_retry(query):
    with s2.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
```

## Best Practices

### 1. Connection Management

```python
# DO: Use context managers
with s2.connect() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()

# DON'T: Leave connections open
conn = s2.connect()
cursor = conn.cursor()
# ... operations ...
# Missing cleanup!
```

### 2. Parameter Binding

```python
# DO: Use parameterized queries
user_id = 123
cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))

# DON'T: String formatting (SQL injection risk)
cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")
```

### 3. Batch Operations

```python
# DO: Use batch operations for multiple inserts
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
cursor.executemany("INSERT INTO users (id, name) VALUES (%s, %s)", data)

# DON'T: Multiple individual inserts
for user_id, name in data:
    cursor.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (user_id, name))
```

### 4. Resource Cleanup

```python
# Proper cleanup pattern
def database_operation():
    conn = None
    try:
        conn = s2.connect()
        cursor = conn.cursor()
        # ... operations ...
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
```

### 5. Environment-Specific Configuration

```python
import os

class DatabaseConfig:
    def __init__(self):
        self.host = os.getenv('SINGLESTORE_HOST', 'localhost')
        self.port = int(os.getenv('SINGLESTORE_PORT', 3306))
        self.user = os.getenv('SINGLESTORE_USER', 'root')
        self.password = os.getenv('SINGLESTORE_PASSWORD', '')
        self.database = os.getenv('SINGLESTORE_DATABASE', 'test')
        self.ssl_ca = os.getenv('SINGLESTORE_SSL_CA')

    def get_connection_url(self):
        url = f"singlestoredb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        if self.ssl_ca:
            url += f"?ssl_ca={self.ssl_ca}"
        return url

# Usage
config = DatabaseConfig()
engine = create_engine(config.get_connection_url())
```

### 6. Monitoring and Logging

```python
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_query_performance(func):
    """Decorator to log query execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Query executed in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query failed after {execution_time:.4f} seconds: {e}")
            raise
    return wrapper

@log_query_performance
def execute_complex_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()
```

## Testing Patterns

### Unit Testing Database Operations

```python
import unittest
from unittest.mock import patch, MagicMock

class TestDatabaseOperations(unittest.TestCase):

    @patch('singlestoredb.connect')
    def test_user_query(self, mock_connect):
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_connection

        # Test the function
        result = get_users()

        # Assertions
        mock_cursor.execute.assert_called_once()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][1], 'Alice')

def get_users():
    with s2.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM users")
            return cursor.fetchall()
```

This comprehensive guide covers the essential aspects of working with the SingleStore Python SDK, from basic connections to advanced patterns and best practices. Use it as a reference for implementing robust database operations in your applications.
