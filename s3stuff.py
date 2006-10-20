"""
s3stuff - s3 facing bits of an AWS backend
"""
import boto
from boto.key import Key
from ZODB.utils import u64, p64, z64
import time
from boto.exception import S3ResponseError
from boto.bucket import Bucket

RETRIES = 5
SLEEPTIME = 0.1
DEBUG = True

class S3Aspect:
    '''A storage for Amazon S3 web service
    '''
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name):
        conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        self._bucket = Bucket(conn, bucket_name)

    def lastCommit(self):
        '''
        Get the last committed tid and serial according to S3.
        This may not be current
        '''
        prefix = 'type:index_serial,'
        rs = self._bucket.get_all_keys(prefix=prefix, maxkeys=1)
        if not rs:
            # umm not created yet
            return None, None
        else:
            info = dict_from_key(rs[0].key)
            tid = tid_unrepr(info['tid'])
            serial = serial_unrepr(info['serial'])
        return tid, serial

    def highestOid(self):
        '''
        Get the highest oid value
        '''
        prefix = 'type:record,'
        rs = self._bucket.get_all_keys(prefix=prefix, maxkeys=1)
        if not rs:
            # umm not created yet
            return None
        else:
            info = dict_from_key(rs[0].key)
            oid = oid_unrepr(info['oid'])
        return oid
 
    def getSerialForTid(self, tid):
        '''
        get the serial for a tid
        '''
        prefix = 'type:index_tid,tid:%s,' % tid_repr(tid)
        rs = self._bucket.get_all_keys(prefix=prefix, maxkeys=1)
        if not rs:
            #either it hasn't propogated yet or has been packed away
            return None
        info = dict_from_key(rs[0].key)
        serial = serial_unrepr(info['serial'])
        return serial
    
    def loadPickle(self, oid, tid):
        """Actually get the data from s3"""
        k = Key(self._bucket)
        k.key = 'type:record,oid:%s,tid:%s' % (oid_repr(oid), tid_repr(tid))
        if DEBUG: print 'LOAD  %s' % k.key
        for n in xrange(RETRIES):
            try:
                return k.get_contents_as_string()
            except S3ResponseError:
                if DEBUG: print 'RETRY %s' % n
                time.sleep(SLEEPTIME)
        return k.get_contents_as_string()

        
    def storePickle(self, oid, tid, prev_tid, data):
        '''Save the data to s3'''
        k = Key(self._bucket)
        k.key = 'type:record,oid:%s,tid:%s' % (oid_repr(oid), tid_repr(tid))
        if DEBUG: print 'STORE %s' % k.key
        for n in xrange(RETRIES):
            try:
                return k.set_contents_from_string(data)
            except:
                if DEBUG: print 'RETRY %s' % n                
                time.sleep(SLEEPTIME)
        return k.set_contents_from_string(data)
            
    def findLastCommit(self, oid, serial):
        '''
        Look for the last committed record for oid committed on or before serial
        '''
        oid_key = oid_repr(oid)
        serial_key = serial_repr(serial)
        prefix = 'type:index_oid,oid:%s,' % oid_key
        marker = 'type:index_oid,oid:%s,serial:%s,' % (oid_key, serial_key)
        rs = self._bucket.get_all_keys(prefix=prefix, marker=marker, maxkeys=1)
        if not rs:
            return None # no commits yet
        key = rs[0]
        return dict_from_key(key.key)
        
    def getTrailingTransactions(self, serial):
        '''Get commits after serial'''
        #
        # Lots of reads here. slow!
        #
        prefix = 'type:transaction,'
        # use a / as it is later than a ,
        marker = 'type:transaction,order:%s/' % order_repr(serial)
        rs = self._bucket.get_all_keys(prefix=prefix, marker=marker)
        data = []
        for key in rs:
            info = dict_from_key(key.key)
            packed = key.get_contents_as_string()
            assert len(packed) % 8 == 0 # 8 byte packed oids
            oids = set(packed[n:n+8] for n in xrange(0, len(packed), 8))
            assert len(oids) == len(packed) / 8
            data.append((info, oids))
        return data
        
    def writeTransaction(self, tid, serial, oids, ude):
        packed = ''.join(oid for oid in oids)
        k = Key(self._bucket)
        k.key = 'type:transaction,order:%s,tid:%s' % (order_repr(serial),
                                                      tid_repr(tid))
        if DEBUG: print 'COMMIT %s' %k.key
        k.set_contents_from_string(packed)
        

    def indexTransaction(self, tid, serial, oids):
        for oid in oids:
            k = Key(self._bucket)
            k.key = 'type:index_oid,oid:%s,serial:%s,tid:%s' %(
                oid_repr(oid),
                serial_repr(serial),
                tid_repr(tid))
            k.set_contents_from_string('')
        k = Key(self._bucket)

        # Index tid to serial
        k.key = 'type:index_tid,tid:%s,serial:%s' %(
                tid_repr(tid),
                serial_repr(serial))
        k.set_contents_from_string('')

        # Index the highest committed serial
        k = Key(self._bucket)
        k.key = 'type:index_serial,serial:%s,tid:%s' %(
                serial_repr(serial),
                tid_repr(tid))
        k.set_contents_from_string('')

# Convert tids, oids, serials, orders from packed 8 byte strings to an a string
# representation suitable for storing in S3
MAXINT = 0xffffffffffffffff
assert MAXINT == 2**64 -1

def invertid(id):
    assert id >= 0
    return MAXINT - id

def hexrepr(id):
    return '%016x' % id

def unhexrepr(s):
    '''
    convert an hexrepr string back to an integer
    '''
    assert len(s) == 16
    return int(s, 16)    

def serial_repr(id):
    '''
    string representation of a serial, so that numerically higher values are
    alphabetically lower.
    '''
    return hexrepr(invertid(u64(id)))

def serial_unrepr(s):
    return p64(invertid(unhexrepr(s)))

def tid_repr(tid):
    return hexrepr(u64(tid))

def tid_unrepr(tid_key):
    return p64(unhexrepr(tid_key))

order_repr = tid_repr
order_unrepr = tid_unrepr

oid_repr = serial_repr
oid_unrepr = serial_unrepr

def dict_from_key(s):
    items = s.split(',')
    d = dict()
    for i in items:
        k, v = i.split(':')
        d[k] = v
    return d
