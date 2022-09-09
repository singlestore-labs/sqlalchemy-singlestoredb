.. currentmodule:: sqlalchemy_singlestoredb

Getting Started
===============

Connections to SingleStoreDB are made using URLs just like any other
SQLAlchemy dialect.

.. ipython:: python
   :verbatim:

   from sqlalchemy import create_engine

   eng = create_engine('singlestoredb://user:password@host.com:port/database')

   with eng.connect() as conn:
        res = conn.execute(...)
        print(res)

More examples of connection usage can be found at the
`SQLAlchemy <https://docs.sqlalchemy.org/en/14/index.html>`_ site.
