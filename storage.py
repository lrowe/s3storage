"""
s3storage - an amazon web services zodb backend
"""

import base64, time, threading, transaction, cPickle, logging
from ZODB import ConflictResolution, POSException
from ZODB.BaseStorage import BaseStorage
from BTrees import OOBTree
from ZODB.utils import u64, p64, z64

from s3stuff import S3Aspect, tid_unrepr, order_unrepr
from mcstuff import MemcacheAspect

log = logging.getLogger("S3Storage")

class S3StorageError(Exception):
    pass

class S3Storage(BaseStorage,
                ConflictResolution.ConflictResolvingStorage):
    '''A storage for Amazon S3 web service
    '''
    def __init__(self, name, aws_access_key_id,
                 aws_secret_access_key, bucket_name, memcache_servers,
                 read_only=False):
        BaseStorage.__init__(self, name)
        #import pdb; pdb.set_trace()
        self._s3 = S3Aspect(aws_access_key_id, aws_secret_access_key,
                            bucket_name)
        self._memcache = MemcacheAspect(memcache_servers.split(' '),
                                        'bucket:%s' % bucket_name)        
        if read_only:
            self._is_read_only = True

        self._clear_temp()
        #self._tid = None # the current tid
        #self._ts = None # the current timestamp
        #self._transaction = None # the current transaction

        self._ltid = None
        self._lserial = None

        # setup memcache
        ltid, lserial = self._s3.lastCommit()
        if ltid is None:
            ltid, lserial = z64, z64
        oid = self._s3.highestOid()
        if oid is None:
            oid = z64
        self._memcache.setup(ltid, lserial, oid)

        
    def _zap(self):
        #
        # blank it all!
        #
        for k in self._s3._bucket.get_all_keys():
            self._s3._bucket.delete_key(k)

    def closing_connection(self):
        """Reset the transaction state on connection close."""
        self._ltid = None
        self._lserial = None

    def _clear_temp(self):
        #self._ltid = None # last committed tid at transaction begin
        #self._lserial = None # read cursor, serial of ltid
        self._wtid = None # last commited tid at tpc_begin, >= ltid
        self._wserial = None # write cursor, serial of wtid
        self._ftid =None # last committed tid at tpc_vote, >= wtid
        self._fserial = None # the finalize cursor, serial of ftid
        self._fwdlog = []
        self._dirty = set()

##     def sync(self):
##         '''
##         Update our idea of the last committed transaction
##         I think this will get called for each thread at the beginning of each
##         transaction and at the end
##         '''
##         self._lock_acquire()
##         try:
##             #ltid, lserial = self._getLiveTidSerial()
##             #self._thread.ltid = ltid
##             #lserial = self._getSerialForTid(ltid)
##             # tid is assigned at tpc_begin
##         finally:
##             self._lock_release()

    def lastTransaction(self):
        '''
        Get the last transaction
        '''
        return self._ltid

    def _lastCommit(self):
        '''
        Get the last committed tid and serial from memcache, looking it up in
        s3 if necessary
        '''
        return self._memcache.lastCommit()

    def new_oid(self):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        return self._memcache.takeOidTicket()
        
    def loadSerial(self, oid, serial):
        """Load a specific revision of an object"""
        # serial == tid in zodb. prob not used.
        return self._s3.loadPickle(oid, serial)
    
    def loadBefore(self, oid, tid):
        """Return most recent revision of oid before tid committed."""
        #TODO check recent cache first
        max_serial = p64(u64(self._s3.getSerialForTid(tid)) - 1)
        keydata = self._s3.findLastCommit(oid, max_serial)

        oid = oid_unrepr(keydata['oid'])
        serial = serial_unrepr(keydata['serial'])
        #get the pickle
        data = self._s3.loadPickle(oid, serial)


        # need another index for this
        #get the end date (should check recent cache first)
        #prefix = 'type:commit,'
        #marker = 'type:commit,oid:%s,' % oid_key
        #rs = self._bucket.get_all_keys(prefix=prefix, marker=marker,
        #                               maxkeys=1)
        #TODO error handling
        #assert len(keys) == 1
        #key = rs[0]
        #enddata = dict_from_key(key.key)
        #if enddata['tid'] > keydata['tid']:
        #    end = p64(serial_unrepr(enddata['tid']))
        #else:
        #    end = None
        end = None

        start = tid_unrepr(keydata['tid'])

        return data, start, end
            
    def load(self, oid, version):
        """Return most recent revision of oid, at txn begin time"""
        if not self._lserial:
            # first access, start a read transaction
            self._ltid, self._lserial = self._lastCommit()
        
        #TODO check recent cache first
        keydata = self._s3.findLastCommit(oid, self._lserial)
        if keydata is None:
            raise KeyError(oid)
        tid = tid_unrepr(keydata['tid'])
        #get the pickle
        data = self._s3.loadPickle(oid, tid)
        return data, tid
        # pickle, serial
        
    def loadEx(self, oid, version):
        # Since we don't support versions, just tack the empty version
        # string onto load's result.
        return self.load(oid, version) + ("",)
        
    def store(self, oid, serial, data, version, transaction):
        # I think serial must be the tid that we read the object at
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise POSException.Unsupported("Versions aren't supported")

        prev_tid = z64
        if not serial:
            serial = z64
        
        info = self._s3.findLastCommit(oid, self._wserial)
        if info:
            prev_tid = tid_unrepr(info['tid'])
            if prev_tid != serial:
                # conflict detected
                rdata = self.tryToResolveConflict(
                    oid, prev_tid, serial, data)
                if rdata is None:
                    raise POSException.ConflictError(
                        message='conflict error in store',
                        oid=oid, serials=(prev_tid, serial), data=data)
                else:
                    data = rdata

        self._s3.storePickle(oid, self._tid, prev_tid, data)
        self._dirty.add(oid)

        if prev_tid and serial != prev_tid:
            return ConflictResolution.ResolvedSerial
        else:
            return self._tid

    def tpc_begin(self, transaction, tid=None, status=' '):
        if self._is_read_only:
            raise POSException.ReadOnlyError()
        self._lock_acquire()
        try:
            if self._transaction is transaction:
                return
            self._lock_release()
            self._commit_lock_acquire()
            self._lock_acquire()
            self._transaction = transaction
            self._clear_temp()

            user = transaction.user
            desc = transaction.description
            ext = transaction._extension
            if ext:
                ext = cPickle.dumps(ext, 1)
            else:
                ext = ""
            self._ude = user, desc, ext

            if tid is None:
                #now = time.time()
                #t = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
                #self._ts = t = t.laterThan(self._ts)
                #self._tid = `t`
                self._tid = self._memcache.new_tid()
            else:
                #self._ts = TimeStamp(tid)
                self._tid = tid

            self._tstatus = status
            self._begin(self._tid, user, desc, ext)
        finally:
            self._lock_release()
    

    def _begin(self, tid, u, d, e):
        """Set up the info for the write cursor
        """
        self._wtid, self._wserial = self._lastCommit()
        #import pdb; pdb.set_trace()


    #
    # stores happen now
    #

    def _sync_forward_log(self):
        if self._fwdlog:
            info, oids = self._fwdlog[-1]
            serial = order_unrepr(info['order'])
            ctid, cserial = self._lastCommit()
            if serial == cserial:
                # nothing to do
                return
        else:
            serial = self._wserial
        newer = self._s3.getTrailingTransactions(serial) # Need to look at mc
        self._fwdlog.extend(newer)

    def _vote(self):
        # get the commit lock
        # now need to check for serialization errors
        # that is someone else has committed an object since tpc_begin with the
        # same oid as we are about to commit. Could we try resolving again?
        self._sync_forward_log()
        self._memcache.serialize_lock.acquire()
        self._serial = self._memcache.takeSerialTicket()
        self._sync_forward_log()
        for info, oids in self._fwdlog: # could be optimised
            conflicts = self._dirty.intersection(oids)
            if conflicts:
                self._memcache.serialize_lock.release() # release asap
                prev_tid = tid_unrepr(info['tid'])
                serial = self._lserial # what if we resolved??
                #import pdb; pdb.set_trace()
                raise POSException.ConflictError(
                    message = 'conflict error in vote',
                    oid=conflicts.pop(), serials=(prev_tid, self._tid),
                    data=None)

    def _finish(self, tid, u, d, e):
        # write the transaction file to s3, this commits the transaction
        # release the serialize lock
        self._s3.writeTransaction(self._tid, self._serial, self._dirty,
                                  self._ude)
        self._memcache.setLastCommit(self._tid, self._serial)
        self._memcache.serialize_lock.release()
        self._s3.indexTransaction(self._tid, self._serial, self._dirty)
        self._ltid, self._lserial = None, None

         
    def _abort(self):
        """Subclasses should redefine this to supply abort actions"""
        self._memcache.serialize_lock.release_if_acquired()
        self._ltid, self._lserial = None, None

    def check_invalidations(self, last_tid, ignore_tid=None):
        """Looks for OIDs of objects that changed after the commit of last_tid.

        Returns ({oid: 1}, new_tid).  Once the invalidated
        objects have been flushed, the cache will be current as of
        new_tid; new_tid can be None if no transactions have yet
        committed.  If last_tid is None, this function returns an
        empty invalidation list.  If ignore_tid is provided, the given
        transaction will not be considered when listing OIDs.

        If all objects need to be invalidated because last_tid is not
        found (presumably it has been packed), returns (None, new_tid).
        """
        # find out the most recent transaction ID
        new_tid, new_serial = self._lastCommit()
        
        # Expect to get something back
        assert new_tid is not None

        if new_tid == last_tid:
            # No transactions have been committed since last_tid.
            return {}, last_tid

        if last_tid is None:
            # No last_tid, so presumably there are no objects to
            # invalidate yet.
            return {}, new_tid

        last_serial = self._s3.getSerialForTid(last_tid)
        if last_serial is None:
            # Transaction not found; perhaps it has been packed.
            # Indicate that the connection cache needs to be cleared.
            return None, new_tid

        # Get the list of changed OIDs and return it.
        newer = self._s3.getTrailingTransactions(last_serial)
        # Need to look at mc too

        dirty = {}
        for info, oids in newer:
            if ignore_tid == tid_unrepr(info['tid']):
                continue
            dirty.update(dict((k, 1) for k in oids))
        
        return dirty, new_tid

# Need to have this as one storage per connection I think, like pgstorage
import weakref
class ConnectionSpecificStorage(S3Storage):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._instances = []
        S3Storage.__init__(self, *args, **kwargs)

    def close(self):
        for wref in self._instances:
            instance = wref()
            if instance is not None:
                instance.close()

    def get_instance(self):
        """Get a connection-specific storage instance.
        """
        instance = S3Storage(*self._args, **self._kwargs)
        self._instances.append(weakref.ref(instance))
        return instance

    #def registerDB(self, db, limit):
    #    pass # we don't care

    def load(self, oid, version):
        # handle the root object creation
        if oid != z64:
            raise S3StorageError
        else:
            return S3Storage.load(self, oid, version)

    def store(self, oid, serial, data, version, transaction):
        # handle the root object creation
        if oid != z64:
            raise S3StorageError
        else:
            return S3Storage.store(self, oid, serial, data, version, transaction)
