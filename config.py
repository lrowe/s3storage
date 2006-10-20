from ZODB.config import BaseConfig, ZODBDatabase
from storage import ConnectionSpecificStorage
from connection import StoragePerConnection

class S3StorageDatatype(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        return ConnectionSpecificStorage(
            name=self.name,
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
            bucket_name=self.config.bucket_name,
            memcache_servers=self.config.memcache_servers)

class S3ZODBDatabaseType(ZODBDatabase):
    """Open a database with storage per connection customizations"""
    def open(self):
        import pdb; pdb.set_trace()
        db = ZODBDatabase.open(self)
        assert hasattr(db, 'klass')
        db.klass = StoragePerConnection
        return db
