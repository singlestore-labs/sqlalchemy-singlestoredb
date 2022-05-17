.. module:: singlestore
.. _api:

API Reference
=============

.. _api.functions:


Connections
-----------

Connections to SingleStore should be done by creating an SQLAlchemy engine
first, then calling the :meth:`connect` method on that engine. See the
Getting Started section for more information.

The dialect class below is used by SQLAlchemy, but is typically
not worked with directly.


Dialect
.......

.. currentmodule:: sqlalchemy_singlestore.base

.. autosummary::
   :toctree: generated/

   SingleStoreDialect
   SingleStoreDialect.dbapi
   SingleStoreDialect.import_dbapi
   SingleStoreDialect.initialize
   SingleStoreDialect.create_connect_args
