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
from typing import Optional

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
        _docker_server_instance = docker.start()
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


def get_connection_url(_docker_server: Optional[Any] = None) -> str:
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
    url = os.environ.get('SINGLESTOREDB_URL')
    if url:
        # Format the URL properly
        if url.startswith('http://') or url.startswith('https://'):
            return 'singlestoredb+' + url
        elif not url.startswith('singlestoredb://'):
            return 'singlestoredb://' + url
        return url

    # If no env var, use Docker server if available
    if _docker_server:
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


@pytest.fixture(scope='session')
def base_connection_url(_docker_server: Optional[Any]) -> str:
    """Get the base connection URL without a specific database."""
    url = get_connection_url(_docker_server)
    # Remove any database name from the URL
    if '://' in url:
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
        with conn.begin():
            conn.execute(text(f'CREATE DATABASE IF NOT EXISTS {db_name}'))

    # Yield the database name for the test to use
    yield db_name

    # Cleanup: Drop the test database
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f'DROP DATABASE IF EXISTS {db_name}'))

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
            with conn.begin():
                conn.execute(text(f'DROP TABLE IF EXISTS {table}'))


@pytest.fixture
def dialect() -> Any:
    """Create a fresh SingleStoreDBDialect instance for testing."""
    from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
    return SingleStoreDBDialect()
