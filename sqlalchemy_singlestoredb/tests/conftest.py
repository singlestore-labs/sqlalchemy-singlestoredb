"""
Pytest configuration and fixtures for SingleStoreDB tests.

Provides fixtures for database connection management and test isolation.
"""
from __future__ import annotations

import os
import random
import string
from typing import Any
from typing import Generator

import pytest
import singlestoredb
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_connection_url() -> str:
    """Get the SingleStoreDB connection URL from environment variable."""
    url = os.environ.get('SINGLESTOREDB_URL')
    if not url:
        pytest.skip(
            'Environment variable SINGLESTOREDB_URL is not set. '
            'Please set it to run the tests.',
        )
        # This line is never reached due to pytest.skip, but helps mypy
        return ''
    if not url.startswith('singlestoredb://'):
        return 'singlestoredb://' + url
    return url


def generate_test_db_name() -> str:
    """Generate a random test database name."""
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f'test_sqlalchemy_{random_suffix}'


@pytest.fixture(scope='session')
def base_connection_url() -> str:
    """Get the base connection URL without a specific database."""
    url = get_connection_url()
    # Remove any database name from the URL
    if '/' in url.split('://', 1)[1]:
        # Has a database specified
        parts = url.split('/')
        return '/'.join(parts[:-1])
    return url


@pytest.fixture(scope='function')
def test_database(base_connection_url: str) -> Generator[str, None, None]:
    """Create a test database with a random name and clean it up after the test."""
    db_name = generate_test_db_name()

    # Connect without specifying a database
    engine = create_engine(base_connection_url)

    # Create the test database
    with engine.connect() as conn:
        # Use exec_driver_sql for DDL operations that should auto-commit
        conn.exec_driver_sql(f'CREATE DATABASE IF NOT EXISTS {db_name}')

    # Yield the database name for the test to use
    yield db_name

    # Cleanup: Drop the test database
    with engine.connect() as conn:
        # Use exec_driver_sql for DDL operations that should auto-commit
        conn.exec_driver_sql(f'DROP DATABASE IF EXISTS {db_name}')

    engine.dispose()


@pytest.fixture(scope='function')
def test_engine(
    base_connection_url: str, test_database: str,
) -> Generator[Engine, None, None]:
    """Create a SQLAlchemy engine connected to the test database."""
    # Create engine with the test database
    test_url = f'{base_connection_url}/{test_database}'
    engine = create_engine(test_url)

    yield engine

    # Cleanup: dispose of the engine
    engine.dispose()


@pytest.fixture(scope='function')
def test_connection(
    test_engine: Engine,
) -> Generator[singlestoredb.Connection, None, None]:
    """Create a raw singlestoredb connection to the test database."""
    # Get the URL from the engine
    url = test_engine.url.render_as_string(hide_password=False)
    connection = singlestoredb.connect(url)

    yield connection

    # Cleanup: close the connection if it's not already closed
    try:
        if not connection._closed:
            connection.close()
    except Exception:
        # Connection might already be closed or in error state
        pass


@pytest.fixture(scope='function')
def clean_tables(test_engine: Engine) -> Generator[None, None, None]:
    """Fixture that cleans up any tables created during the test."""
    yield

    # After the test, drop all tables in the test database
    with test_engine.connect() as conn:
        # Get all table names
        result = conn.execute(
            text(
                'SELECT table_name FROM information_schema.tables '
                "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
            ),
        )
        tables = [row[0] for row in result]

        # Drop each table
        for table in tables:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS {table}')


@pytest.fixture
def dialect() -> Any:
    """Create a fresh SingleStoreDBDialect instance for testing."""
    from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
    return SingleStoreDBDialect()
