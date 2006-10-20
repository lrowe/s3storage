"""
s3storage - an amazon web services zodb backend
"""
import memcache
from ZODB.utils import u64, p64, z64
from persistent.TimeStamp import TimeStamp
import time
from random import random

class MemcacheError(Exception):
    pass

class MemcacheAspect:
    '''
    Memcache facing bits
    '''
    def __init__(self, servers, prefix):
        self._mc = memcache.Client(servers)
        self._prefix = prefix
        self.serialize_lock = MemcacheLock(self._mc,
                                           self._prefix+',serialize_lock')
        self.tid_lock = MemcacheLock(self._mc,
                                     self._prefix+',tid_lock')

    def setup(self, tid, serial, oid):
        # should be safe to run multiple times
        self._mc.add('%s,ltid,lserial' % self._prefix, tid + serial)
        self._mc.add('%s,serial' % self._prefix, u64(serial))
        self._mc.add('%s,oid' % self._prefix, u64(oid))

    def lastCommit(self):
        '''
        Memcache idea of the last commited tid and serial
        '''
        key = '%s,ltid,lserial' % self._prefix
        result = self._mc.get(key)
        if result is None:
            raise MemcacheError
        tid = result[:8]
        serial = result[8:]
        return tid, serial
        
    def setLastCommit(self, tid, serial):
        key = '%s,ltid,lserial' % self._prefix
        result = self._mc.replace(key, tid + serial)
        if not result:
            raise MemcacheError
    
    def takeSerialTicket(self):
        key = '%s,serial' % self._prefix
        serial = self._mc.incr(key)
        if serial is None:
            raise MemcacheError
        if serial >= 2**31:
            raise MemcacheError # overflow
        return p64(serial)

    def takeOidTicket(self):
        key = '%s,oid' % self._prefix
        oid = self._mc.incr(key)
        if oid is None: # actually I think we might get a value error
            raise MemcacheError
        if oid >= 2**31:
            raise MemcacheError # overflow
        return p64(oid)


    def new_tid(self):
        #
        # Probably better off using a counter as a tid
        #
        now = time.time()
        t = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
        
        #get the latest timestamp from memcache
        key = '%s,tid' % self._prefix
        self.tid_lock.acquire()
        try:
            result = self._mc.get(key)
            if result:
                t = t.laterThan(result)
            tid = repr(t)
            result = self._mc.replace(key, tid)
            if not result:
                raise MemcacheError
            return tid
        finally:
            self.tid_lock.release()


SLEEP_TIME_DIVISOR = 100 # sleep up to 1/n seconds

# Horrible kludge, need to think about how best to poll here
class MemcacheLock:
    '''
    Locking in memcache. Make sure memcache is in -M mode!

    Might want to make this hold a threading lock on acquire through to release
    '''
    def __init__(self, client, key, sleep_divisor=SLEEP_TIME_DIVISOR):
        self._client = client
        self._key = key
        self._divisor = sleep_divisor
        self._acquired = 0

    def acquire(self, blocking=True):
        #sanity check
        assert not self._acquired
        self._acquired = self._nb_acquire()
        if blocking:
            while not self._acquired: # no alternative to polling here
                time.sleep(random()/self._divisor)
                self._acquired = self._nb_acquire()
        return self._acquired

    def _nb_acquire(self):
        return self._client.add(self._key, '1')

    def release(self):
        #sanity check
        assert self._acquired
        self._client.delete(self._key)
        self._acquired = 0

    def release_if_acquired(self):
        if self._acquired:
            self._client.delete(self._key)
            self._acquired = 0
