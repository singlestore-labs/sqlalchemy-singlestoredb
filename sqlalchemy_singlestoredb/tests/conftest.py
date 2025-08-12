"""
Pytest configuration and fixtures for SingleStoreDB tests.

Provides fixtures for database connection management and test isolation.
"""
from __future__ import annotations

import os
import random
import re
import string
from typing import Any
from typing import Generator
from typing import Optional
from urllib.parse import urlparse

import pytest
import singlestoredb
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Global variable to store Docker server instance
_docker_server_instance = None


@pytest.fixture(scope='session')
def _docker_server() -> Optional[Any]:
    """
    Start SingleStoreDB Docker container if SINGLESTOREDB_URL is not set.

    This fixture has session scope, so the container is started once
    and reused for all tests in the session.
    """
    global _docker_server_instance

    # Check if we should use an existing server
    if os.environ.get('SINGLESTOREDB_URL'):
        yield None
        return

    # Try to start Docker container
    try:
        from singlestoredb.server import docker
        import time

        print('\nStarting SingleStoreDB Docker container for testing...')

        url = os.environ.get('SINGLESTOREDB_URL')

        _docker_server_instance = docker.start()

        if url is None:
            del os.environ['SINGLESTOREDB_URL']
        elif url:
            os.environ['SINGLESTOREDB_URL'] = url

        if os.environ.get('USE_DATA_API', '0').lower() in ('1', 'true', 'on'):
            conn_url = _docker_server_instance.http_connection_url
        else:
            conn_url = _docker_server_instance.connection_url

        print(f'Docker container started. Connection URL: {conn_url}')

        # Wait for the container to be ready by testing the connection
        print('Waiting for SingleStoreDB to be ready...')
        max_retries = 30  # 30 seconds max wait time
        retry_interval = 1  # Check every 1 second

        for attempt in range(max_retries):
            try:
                # Test connection to verify server is ready
                test_conn = _docker_server_instance.connect()
                test_conn.close()
                print(f'SingleStoreDB is ready! (took {attempt + 1} seconds)')
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    # Last attempt failed
                    raise Exception(
                        f'SingleStoreDB container failed to become ready after '
                        f'{max_retries} seconds. Last error: {e}',
                    )
                print(
                    f'Container not ready yet, retrying... '
                    f'({attempt + 1}/{max_retries})',
                )
                time.sleep(retry_interval)

        yield _docker_server_instance
    except Exception as e:
        # If Docker fails, provide helpful error message
        pytest.fail(
            f'Failed to start SingleStoreDB Docker container: {e}\n'
            'Please either:\n'
            '1. Set SINGLESTOREDB_URL environment variable to use an existing server\n'
            '2. Ensure Docker is installed and running for automatic container support',
        )
    finally:
        # Clean up Docker container
        if _docker_server_instance:
            try:
                print('\nStopping SingleStoreDB Docker container...')
                _docker_server_instance.stop()
                _docker_server_instance = None
            except Exception as e:
                print(f'Warning: Failed to stop Docker container: {e}')


@pytest.fixture(scope='session')
def base_connection_url(_docker_server: Optional[Any]) -> str:
    """
    Get the SingleStoreDB connection URL from environment variable or Docker.

    Parameters
    ----------
    _docker_server : Optional Docker server instance from fixture

    Returns
    -------
    str : Connection URL for SingleStoreDB
    """
    # First check environment variable
    url = os.environ.get('SINGLESTOREDB_URL', '').strip()
    if url:
        print(f'Using SINGLESTOREDB_URL from environment: {url}')
        return ensure_standard_url(url)

    # If no env var, use Docker server if available
    if _docker_server:
        if os.environ.get('USE_DATA_API', '0').lower() in ('1', 'true', 'on'):
            # Use Data API connection URL
            print(
                'Using SingleStoreDB Docker container with Data API connection URL: ' +
                _docker_server.http_connection_url,
            )
            return _docker_server.http_connection_url
        print(
            'Using SingleStoreDB Docker container with direct connection URL: ' +
            _docker_server.connection_url,
        )
        return _docker_server.connection_url

    # Neither env var nor Docker available
    raise ValueError(
        'No SingleStoreDB connection available. '
        'Either set SINGLESTOREDB_URL environment variable or ensure Docker is running.',
    )


def generate_test_db_name() -> str:
    """Generate a random test database name."""
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f'test_sqlalchemy_{random_suffix}'


def generate_table_name_prefix() -> str:
    """Generate a random table name prefix for test isolation."""
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f'tbl_{random_suffix}_'


def ensure_sqlalchemy_url(url: str) -> str:
    """
    Ensure the URL is in the correct format for SQLAlchemy.

    Parameters
    ----------
    url : str
        The connection URL to validate.

    Returns
    -------
    str
        The formatted URL ready for SQLAlchemy.
    """
    if url.startswith('http://') or url.startswith('https://'):
        return 'singlestoredb+' + url
    if url.startswith('singlestoredb+') or url.startswith('singlestoredb://'):
        return url
    return 'singlestoredb://' + url


def ensure_standard_url(url: str) -> str:
    """
    Ensure the URL is in standard format for SingleStoreDB.

    Parameters
    ----------
    url : str
        The connection URL to validate.

    Returns
    -------
    str
        The formatted URL ready for SingleStoreDB.
    """
    return re.sub(r'^(singlestoredb\+|singlestoredb://)', '', url, count=1)


def get_url_components(url: str) -> tuple[str, str, str]:
    """
    Extract components from a SingleStoreDB connection URL.

    Parameters
    ----------
    url : str
        The connection URL to parse.

    Returns
    -------
    tuple[str, str, str, str]
        A tuple containing (base_url, database, query).

    """
    if '://' not in url:
        url = 'singlestoredb://' + url
    parts = urlparse(url)
    return (
        parts.scheme + '://' + parts.netloc,
        parts.path.replace('/', ''),
        parts.query,
    )


@pytest.fixture(scope='session')
def test_database(base_connection_url: str) -> Generator[str, None, None]:
    """Create a single test database for the entire test session."""

    # If SINGLESTOREDB_INIT_DB_URL is set, use it to create the database
    db_connection_url = base_connection_url
    if os.environ.get('SINGLESTOREDB_INIT_DB_URL', '').strip():
        db_connection_url = ensure_standard_url(
            os.environ['SINGLESTOREDB_INIT_DB_URL'].strip(),
        )

    db_connection_url = ensure_standard_url(db_connection_url)
    base_connection_url = ensure_standard_url(base_connection_url)

    print(f'Using base connection URL: {db_connection_url} for database creation')

    # If the URL specifies a database, use it as-is
    has_database = False
    _, db_name, _ = get_url_components(base_connection_url)
    if db_name:
        has_database = True
    else:
        db_name = generate_test_db_name()

    # Create the test database
    if not has_database:
        with singlestoredb.connect(db_connection_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
        print(f'Created shared test database: {db_name}')
    else:
        print(f'Using existing database from connection URL: {db_name}')

    # Yield the database name for all tests to use
    yield db_name

    # Cleanup: Drop the test database at the end of the session
    if not has_database:
        print(f'Cleaning up shared test database: {db_name}')
        with singlestoredb.connect(db_connection_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE IF EXISTS {db_name}')


@pytest.fixture(scope='session')
def test_engine(
    base_connection_url: str, test_database: str,
) -> Generator[Engine, None, None]:
    """Create a SQLAlchemy engine connected to the shared test database."""
    base_connection_url = ensure_sqlalchemy_url(base_connection_url)
    base_url, db_name, query = get_url_components(base_connection_url)

    test_url = f'{base_url}/{test_database}'
    if query:
        test_url += f'?{query}'

    print(f'Using engine URL: {test_url}')
    engine = create_engine(test_url)

    yield engine

    # Cleanup: dispose of the engine at session end
    engine.dispose()


@pytest.fixture(scope='function')
def table_name_prefix() -> str:
    """Generate a unique table name prefix for each test."""
    return generate_table_name_prefix()


@pytest.fixture(scope='function')
def test_connection(
    test_engine: Engine,
) -> Generator[singlestoredb.connection.Connection, None, None]:
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
def clean_tables(
    test_engine: Engine, table_name_prefix: str,
) -> Generator[None, None, None]:
    """Fixture that cleans up tables created with the test's table prefix."""
    # Track tables that exist before the test
    with test_engine.connect() as conn:
        result = conn.execute(
            text(
                'SELECT table_name FROM information_schema.tables '
                "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' "
                f"AND table_name LIKE '{table_name_prefix}%'",
            ),
        )
        existing_tables = {row[0] for row in result}

    yield

    # After the test, drop only tables created by this test (with our prefix)
    with test_engine.connect() as conn:
        with conn.begin():
            result = conn.execute(
                text(
                    'SELECT table_name FROM information_schema.tables '
                    "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' "
                    f"AND table_name LIKE '{table_name_prefix}%'",
                ),
            )
            current_tables = {row[0] for row in result}

            # Drop only newly created tables (those not in existing_tables)
            new_tables = current_tables - existing_tables
            for table in new_tables:
                conn.execute(text(f'DROP TABLE IF EXISTS {table}'))


@pytest.fixture
def dialect() -> Any:
    """Create a fresh SingleStoreDBDialect instance for testing."""
    from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
    return SingleStoreDBDialect()
