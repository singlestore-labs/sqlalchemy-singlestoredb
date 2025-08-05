"""
SQLAlchemy version compatibility utilities for SingleStoreDB dialect.

This module provides version detection and compatibility abstractions
to support both SQLAlchemy 1.4+ and 2.0+ in the same codebase.
"""
from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING

import sqlalchemy
from sqlalchemy import __version__ as sqlalchemy_version

# Version detection
SQLALCHEMY_VERSION = tuple(int(x) for x in sqlalchemy_version.split('.')[:2])
SQLALCHEMY_1_4 = SQLALCHEMY_VERSION >= (1, 4)
SQLALCHEMY_2_0 = SQLALCHEMY_VERSION >= (2, 0)

# Conditional imports based on SQLAlchemy version
if SQLALCHEMY_2_0:
    from sqlalchemy.sql.expression import Insert as SQLAlchemyInsert
    from sqlalchemy.sql.expression import Update as SQLAlchemyUpdate
    from sqlalchemy.sql.expression import Delete as SQLAlchemyDelete
    from sqlalchemy.engine import Engine
    from sqlalchemy.pool import Pool
    try:
        from sqlalchemy.sql.expression import CacheKey
        HAS_CACHE_KEY = True
    except ImportError:
        HAS_CACHE_KEY = False
        CacheKey = None
else:
    # SQLAlchemy 1.4 imports
    from sqlalchemy.sql.dml import Insert as SQLAlchemyInsert
    from sqlalchemy.sql.dml import Update as SQLAlchemyUpdate
    from sqlalchemy.sql.dml import Delete as SQLAlchemyDelete
    from sqlalchemy.engine import Engine
    from sqlalchemy.pool import Pool
    try:
        from sqlalchemy.sql.base import CacheKey
        HAS_CACHE_KEY = True
    except ImportError:
        HAS_CACHE_KEY = False
        CacheKey = None

if TYPE_CHECKING:
    from sqlalchemy.sql.base import CacheKey as CacheKeyType
else:
    CacheKeyType = Any


def get_sqlalchemy_version() -> tuple[int, int]:
    """Get the current SQLAlchemy version as a tuple (major, minor)."""
    return SQLALCHEMY_VERSION


def has_feature(feature_name: str) -> bool:
    """Check if a specific SQLAlchemy feature is available."""
    feature_map = {
        'cache_key': HAS_CACHE_KEY,
        'insertmanyvalues': SQLALCHEMY_2_0,
        'server_side_cursors': SQLALCHEMY_1_4,
        'statement_cache': SQLALCHEMY_1_4,
        'execution_options_isolation_level': SQLALCHEMY_1_4,
    }
    return feature_map.get(feature_name, False)


def get_insert_class() -> Type[SQLAlchemyInsert]:
    """Get the appropriate Insert class for the current SQLAlchemy version."""
    return SQLAlchemyInsert


def get_update_class() -> Type[SQLAlchemyUpdate]:
    """Get the appropriate Update class for the current SQLAlchemy version."""
    return SQLAlchemyUpdate


def get_delete_class() -> Type[SQLAlchemyDelete]:
    """Get the appropriate Delete class for the current SQLAlchemy version."""
    return SQLAlchemyDelete


def make_cache_key(*args: Any, **kwargs: Any) -> Optional[CacheKeyType]:
    """Create a cache key if caching is supported."""
    if not HAS_CACHE_KEY or CacheKey is None:
        return None

    try:
        if SQLALCHEMY_2_0:
            # SQLAlchemy 2.0 cache key creation
            return CacheKey(*args, **kwargs)
        else:
            # SQLAlchemy 1.4 cache key creation
            return CacheKey(*args, **kwargs)
    except Exception:
        return None


def supports_statement_cache() -> bool:
    """Check if statement caching is supported in the current SQLAlchemy version."""
    return SQLALCHEMY_1_4


def get_dialect_features() -> dict[str, Any]:
    """Get a dictionary of available dialect features based on SQLAlchemy version."""
    features = {
        'supports_statement_cache': supports_statement_cache(),
        'supports_server_side_cursors': SQLALCHEMY_1_4,
        'has_cache_key': HAS_CACHE_KEY,
        'sqlalchemy_version': SQLALCHEMY_VERSION,
        'is_sqlalchemy_14': SQLALCHEMY_1_4 and not SQLALCHEMY_2_0,
        'is_sqlalchemy_20': SQLALCHEMY_2_0,
    }

    if SQLALCHEMY_2_0:
        # SQLAlchemy 2.0+ specific features
        features.update({
            'use_insertmanyvalues': True,
            'supports_insertmanyvalues_wo_returning': True,
        })

    return features


# Version-specific attribute access helpers
def get_dialect_attribute(dialect: Any, attr_name: str, default: Any = None) -> Any:
    """Safely get an attribute from a dialect, returning default if not available."""
    return getattr(dialect, attr_name, default)


def set_dialect_attribute(dialect: Any, attr_name: str, value: Any) -> bool:
    """Safely set an attribute on a dialect if supported."""
    try:
        setattr(dialect, attr_name, value)
        return True
    except (AttributeError, TypeError):
        return False


# Compatibility warnings
def warn_version_compatibility(feature: str, min_version: str) -> None:
    """Issue a warning about version compatibility for a specific feature."""
    import warnings
    warnings.warn(
        f"Feature '{feature}' requires SQLAlchemy {min_version}+. "
        f'Current version is {sqlalchemy_version}. Feature will be disabled.',
        UserWarning,
        stacklevel=3,
    )


# Export the key classes and functions for easy import
__all__ = [
    'SQLALCHEMY_VERSION',
    'SQLALCHEMY_1_4',
    'SQLALCHEMY_2_0',
    'SQLAlchemyInsert',
    'SQLAlchemyUpdate',
    'SQLAlchemyDelete',
    'Engine',
    'Pool',
    'CacheKey',
    'HAS_CACHE_KEY',
    'get_sqlalchemy_version',
    'has_feature',
    'get_insert_class',
    'get_update_class',
    'get_delete_class',
    'make_cache_key',
    'supports_statement_cache',
    'get_dialect_features',
    'get_dialect_attribute',
    'set_dialect_attribute',
    'warn_version_compatibility',
]
