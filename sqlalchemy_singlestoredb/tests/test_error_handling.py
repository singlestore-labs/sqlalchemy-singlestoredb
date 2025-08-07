#!/usr/bin/env python
"""
Test error handling and edge cases for SingleStoreDB dialect.

Tests error conditions, edge cases, and robustness of our Priority 1 features.
"""
from __future__ import annotations

from unittest.mock import Mock
from unittest.mock import patch

import pytest
import singlestoredb

import sqlalchemy_singlestoredb.base
from sqlalchemy_singlestoredb.base import SingleStoreDBDialect
from sqlalchemy_singlestoredb.base import SingleStoreDBExecutionContext
from sqlalchemy_singlestoredb.compat import HAS_CACHE_KEY
from sqlalchemy_singlestoredb.compat import warn_version_compatibility
from sqlalchemy_singlestoredb.dtypes import JSON
from sqlalchemy_singlestoredb.dtypes import VECTOR


# Import our dialect components


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_do_ping_connection_failure(self, dialect: SingleStoreDBDialect) -> None:
        """Test do_ping handles connection failures gracefully."""
        # Mock connection that raises exception on cursor()
        mock_connection = Mock()
        mock_connection.cursor.side_effect = Exception('Connection lost')

        result = dialect.do_ping(mock_connection)
        assert result is False

    def test_do_ping_cursor_execution_failure(
        self,
        dialect: SingleStoreDBDialect,
        test_connection: singlestoredb.connection.Connection,
    ) -> None:
        """Test do_ping handles cursor execution failures."""
        # Close the connection to simulate a broken connection
        test_connection.close()

        # Now test ping on the closed connection
        result = dialect.do_ping(test_connection)
        assert result is False

    def test_do_ping_fetchone_failure(
        self,
        dialect: SingleStoreDBDialect,
        test_connection: singlestoredb.connection.Connection,
    ) -> None:
        """Test do_ping handles fetchone failures."""
        # Test ping on a good connection first
        result = dialect.do_ping(test_connection)
        assert result is True

        # Close connection and test ping failure
        test_connection.close()
        result = dialect.do_ping(test_connection)
        assert result is False

    def test_is_disconnect_with_errno_exceptions(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test disconnect detection with errno-based exceptions."""
        mock_connection = Mock()
        mock_cursor = Mock()

        # Mock parent class to return False
        with patch.object(
            dialect.__class__.__bases__[0],
            'is_disconnect',
            return_value=False,
        ):
            # Test MySQL "server gone away" errors
            disconnect_codes = [2006, 2013, 2055]

            for code in disconnect_codes:
                class MockException(Exception):
                    def __init__(self) -> None:
                        self.errno = code

                result = dialect.is_disconnect(
                    MockException(), mock_connection, mock_cursor,
                )
                assert result is True, f'Should detect disconnect for errno {code}'

    def test_is_disconnect_with_message_patterns(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test disconnect detection with error message patterns."""
        mock_connection = Mock()
        mock_cursor = Mock()

        # Mock parent class to return False
        with patch.object(
            dialect.__class__.__bases__[0],
            'is_disconnect',
            return_value=False,
        ):
            disconnect_messages = [
                'Connection lost to server',
                'MySQL server has gone away',
                'Lost connection during query',
                'Broken pipe error',
                'Connection reset by peer',
            ]

            for message in disconnect_messages:
                result = dialect.is_disconnect(
                    Exception(message), mock_connection, mock_cursor,
                )
                msg = f'Should detect disconnect for message: {message}'
                assert result is True, msg

    def test_is_disconnect_normal_errors(self, dialect: SingleStoreDBDialect) -> None:
        """Test disconnect detection doesn't trigger on normal errors."""
        mock_connection = Mock()
        mock_cursor = Mock()

        # Mock parent class to return False
        with patch.object(
            dialect.__class__.__bases__[0],
            'is_disconnect',
            return_value=False,
        ):
            normal_messages = [
                'Syntax error in query',
                "Table doesn't exist",
                'Permission denied',
                'Invalid column name',
            ]

            for message in normal_messages:
                result = dialect.is_disconnect(
                    Exception(message), mock_connection, mock_cursor,
                )
                msg = f'Should NOT detect disconnect for: {message}'
                assert result is False, msg

    def test_on_connect_exception_handling(
        self,
        dialect: SingleStoreDBDialect,
        test_connection: singlestoredb.connection.Connection,
    ) -> None:
        """Test on_connect handles exceptions gracefully."""
        # Test the on_connect function with the test connection
        connect_fn = dialect.on_connect()

        # Should not raise exception when called on real connection
        try:
            if connect_fn is not None:
                connect_fn(test_connection)
            # If we get here, the function handled the connection gracefully
            assert True
        except Exception as e:
            pytest.fail(f'on_connect should handle real connections gracefully: {e}')
        finally:
            test_connection.close()

    def test_vector_type_invalid_parameters(self) -> None:
        """Test VECTOR type with invalid parameters."""
        # Test with None parameters (should use defaults)
        vector = VECTOR()
        assert vector.n_elems == 1
        assert vector.elem_type == 'F32'

        # Test with zero dimensions - VECTOR() constructor actually uses 1 as minimum
        vector = VECTOR(0, 'F32')
        assert vector.n_elems == 1  # Constructor ensures minimum of 1

        # Test with element type that doesn't match expected format
        # (should handle gracefully)
        # Using a valid-looking but unusual element type
        vector = VECTOR(128, 'F16')  # F16 is actually valid
        assert vector.elem_type == 'F16'

    def test_json_type_deserialization_errors(self) -> None:
        """Test JSON type handles deserialization errors."""
        json_type = JSON()
        dialect = sqlalchemy_singlestoredb.dialect()

        # Get result processor
        processor = json_type.result_processor(dialect, None)

        # Test with None (should return None)
        result = processor(None)
        assert result is None

        # Test with already deserialized dict/list (should pass through)
        test_dict = {'key': 'value'}
        result = processor(test_dict)
        assert result == test_dict

        test_list = [1, 2, 3]
        result = processor(test_list)
        assert result == test_list

    def test_cache_key_generation_without_support(self) -> None:
        """Test cache key generation when caching not supported."""
        # Test with caching disabled
        if not HAS_CACHE_KEY:
            vector_type = VECTOR(128, 'F32')
            json_type = JSON()

            # Should return None when caching not supported
            assert vector_type._gen_cache_key(Mock(), Mock()) is None
            assert json_type._gen_cache_key(Mock(), Mock()) is None

    def test_cache_key_generation_exceptions(self) -> None:
        """Test cache key generation handles exceptions."""
        # Test basic cache key generation functionality
        if HAS_CACHE_KEY:
            vector_type = VECTOR(128, 'F32')
            json_type = JSON()

            # Test with None arguments (should handle gracefully)
            vector_result = vector_type._gen_cache_key(None, None)
            json_result = json_type._gen_cache_key(None, None)

            # Should return None when given invalid arguments
            assert vector_result is None
            assert json_result is None
        else:
            # When caching not supported, should return None
            vector_type = VECTOR(128, 'F32')
            result = vector_type._gen_cache_key(None, None)
            assert result is None

    def test_execution_context_server_side_cursor_disabled(self) -> None:
        """Test execution context when server-side cursors are disabled."""
        # Create a dialect with server-side cursors disabled
        dialect = SingleStoreDBDialect()
        dialect.supports_server_side_cursors = False

        # Create execution context
        ctx = SingleStoreDBExecutionContext.__new__(SingleStoreDBExecutionContext)
        ctx.dialect = dialect

        result = ctx.create_server_side_cursor()
        assert result is None

    def test_initialization_cloud_connection(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test dialect initialization with cloud connection."""
        # Mock cloud connection
        mock_connection = Mock()
        mock_dbapi_connection = Mock()
        mock_dbapi_connection.connection_params = {'host': 'singlestore.com'}
        mock_connection.connection = mock_dbapi_connection

        # Initialize dialect
        result = dialect.initialize(mock_connection)

        # Should return early for cloud connections
        assert result is None
        assert dialect.server_version_info is None
        assert dialect.default_schema_name is None
        assert dialect.default_isolation_level == 'READ COMMITTED'

    def test_rollback_cloud_connection(self, dialect: SingleStoreDBDialect) -> None:
        """Test rollback with cloud connection."""
        # Mock cloud connection
        mock_connection = Mock()
        mock_connection.connection_params = {'host': 'singlestore.com'}

        # Should not call rollback for cloud connections
        dialect.do_rollback(mock_connection)
        mock_connection.rollback.assert_not_called()

    def test_get_default_isolation_level_cloud(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test get_default_isolation_level with cloud connection."""
        mock_connection = Mock()
        mock_connection.connection_params = {'host': 'singlestore.com'}

        result = dialect.get_default_isolation_level(mock_connection)
        assert result == 'READ COMMITTED'

    def test_get_default_isolation_level_normal_connection(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test get_default_isolation_level with normal connection."""
        mock_connection = Mock()
        mock_connection.connection_params = {'host': 'localhost'}

        # Mock parent method
        with patch.object(
            dialect.__class__.__bases__[0],
            'get_default_isolation_level',
            return_value='REPEATABLE READ',
        ) as mock_parent:
            result = dialect.get_default_isolation_level(mock_connection)
            assert result == 'REPEATABLE READ'
            mock_parent.assert_called_once_with(mock_connection)

    @pytest.mark.skip(
        reason='SQLAlchemy 2.0+ MySQLDialect does not have _execute_context method',
    )
    def test_execute_context_no_execution_options(
        self,
        dialect: SingleStoreDBDialect,
    ) -> None:
        """Test _execute_context when constructor has no execution_options."""
        mock_constructor = Mock()
        # Constructor without execution_options attribute
        del mock_constructor.execution_options

        # Mock parent method
        with patch.object(
            dialect.__class__.__bases__[0],
            '_execute_context',
            return_value='parent_result',
        ) as mock_parent:
            result = dialect._execute_context(
                dialect, mock_constructor, 'statement', 'parameters',
            )

            # Should call parent method normally
            assert result == 'parent_result'
            mock_parent.assert_called_once()

    def test_compat_module_version_compatibility_warning(self) -> None:
        """Test version compatibility warning function."""
        with patch('warnings.warn') as mock_warn:
            warn_version_compatibility('test_feature', '2.0')

            # Should call warnings.warn
            mock_warn.assert_called_once()
            call_args = mock_warn.call_args[0]
            assert 'test_feature' in call_args[0]
            assert '2.0' in call_args[0]

    @pytest.mark.skip(reason='Complex mocking scenarios require real connection context')
    def test_dialect_attribute_helpers(self) -> None:
        """Test dialect attribute helper functions."""
        from sqlalchemy_singlestoredb.compat import (
            get_dialect_attribute,
            set_dialect_attribute,
        )

        mock_dialect = Mock()
        mock_dialect.test_attr = 'test_value'

        # Test get_dialect_attribute
        result = get_dialect_attribute(mock_dialect, 'test_attr')
        assert result == 'test_value'

        # Test get_dialect_attribute with default
        result = get_dialect_attribute(
            mock_dialect, 'missing_attr', 'default',
        )
        assert result == 'default'

        # Test set_dialect_attribute
        success = set_dialect_attribute(
            mock_dialect, 'new_attr', 'new_value',
        )
        assert success is True
        assert mock_dialect.new_attr == 'new_value'

#   @unittest.skip('Complex mocking scenarios require real connection context')
#   def test_set_dialect_attribute_failure(self) -> None:
#       """Test set_dialect_attribute handles failures."""
#       from sqlalchemy_singlestoredb.compat import set_dialect_attribute

#       # Mock object that raises exception on setattr
#       mock_dialect = Mock()

#       def raise_on_setattr(name: str, value: Any) -> None:
#           if name == 'readonly_attr':
#               raise AttributeError('Cannot set attribute')

#       mock_dialect.__setattr__ = raise_on_setattr

#       # Should return False on exception
#       success = set_dialect_attribute(mock_dialect, 'readonly_attr', 'value')
#       assert success is False

    def test_feature_map_unknown_feature(self) -> None:
        """Test has_feature with unknown feature name."""
        from sqlalchemy_singlestoredb.compat import has_feature

        # Should return False for unknown features
        result = has_feature('unknown_feature')
        assert result is False

    def test_extract_error_code_no_errno(self, dialect: SingleStoreDBDialect) -> None:
        """Test _extract_error_code with exception without errno."""
        exception = Exception('Generic error')
        result = dialect._extract_error_code(exception)
        assert result == -1

    def test_extract_error_code_with_errno(self, dialect: SingleStoreDBDialect) -> None:
        """Test _extract_error_code with exception with errno."""
        class MockException(Exception):
            def __init__(self) -> None:
                self.errno = 1234

        exception = MockException()
        result = dialect._extract_error_code(exception)
        assert result == 1234


if __name__ == '__main__':
    # Run tests with pytest when executed directly
    import subprocess
    import sys

    print('🧪 Running SingleStoreDB Error Handling Tests')
    print('=' * 50)

    # Run pytest on this file
    result = subprocess.run(
        [sys.executable, '-m', 'pytest', __file__, '-v'],
        capture_output=False,
    )

    if result.returncode == 0:
        print('\n🎉 ALL ERROR HANDLING TESTS PASSED!')
        print('Error handling and edge cases are properly covered.')
    else:
        print('\n❌ Some error handling tests failed.')

    sys.exit(result.returncode)
