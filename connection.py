# Copyright (c) 2006 Shane Hathaway.
# Made available under the MIT license; see LICENSE.txt.

"""PGStorage-aware Connection.

Currently, ZODB connections provide MVCC, but PGStorage
provides MVCC in the storage.  PGConnection overrides
certain behaviors in the connection so that the storage can
implement MVCC.
"""

from ZODB.Connection import Connection


class StoragePerConnection(Connection):
    """Connection enhanced with per-connection storage instances."""

    # The connection's cache is current as of self._last_tid.
    _last_tid = None

    # Connection-specific database wrapper
    _wrapped_db = None

    def _wrap_database(self):
        """Link a database to this connection"""
        if self._wrapped_db is None:
            my_storage = self._db._storage.get_instance()
            self._wrapped_db = DBWrapper(self._db, my_storage)
            self._normal_storage = self._storage = my_storage
            self.new_oid = my_storage.new_oid
        self._db = self._wrapped_db

    def _check_invalidations(self, committed_tid=None):
        storage = self._db._storage
        invalid, new_tid = storage.check_invalidations(
            self._last_tid, committed_tid)
        if invalid is None:
            self._resetCache()
        elif invalid:
            self._invalidated.update(invalid)
            self._flush_invalidations()
        self._last_tid = new_tid

    def _setDB(self, odb, *args, **kw):
        """Set up the storage and invalidate before _setDB."""
        self._db = odb
        self._wrap_database()
        self._check_invalidations()
        super(StoragePerConnection, self)._setDB(self._db, *args, **kw)

    def open(self, *args, **kw):
        """Set up the storage and invalidate before open."""
        self._wrap_database()
        self._check_invalidations()
        super(StoragePerConnection, self).open(*args, **kw)

    def tpc_finish(self, transaction):
        """Invalidate upon commit."""
        committed_tids = []
        self._storage.tpc_finish(transaction, committed_tids.append)
        self._check_invalidations(committed_tids[0])
        self._tpc_cleanup()

    def _storage_sync(self, *ignored):
        self._check_invalidations()


class DBWrapper(object):
    """Override the _storage attribute of a ZODB.DB.DB.

    The Connection object uses this wrapper instead of the
    real DB object.  This wrapper allows each connection to
    have its own storage instance.
    """

    def __init__(self, db, storage):
        self._db = db
        self._storage = storage

    def __getattr__(self, name):
        return getattr(self._db, name)

    def _returnToPool(self, connection):
        # This override is for a newer ZODB
        # Reset transaction state in the storage instance.
        self._storage.closing_connection()
        connection._db = self._db  # satisfy an assertion
        self._db._returnToPool(connection)

    def _closeConnection(self, connection):
        # This override is for an older ZODB
        # Reset transaction state in the storage instance.
        self._storage.closing_connection()
        connection._db = self._db  # satisfy an assertion
        self._db._closeConnection(connection)
        
