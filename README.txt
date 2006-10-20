s3storage - zope in the cloud
=============================

Setup
-----

boto from http://code.google.com/p/boto

memcached bindings http://cheeseshop.python.org/pypi/memcached/

start a memcached server with the -M option so it won't garbage collect your
locks.. 

Put this in your zope.conf

%import Products.s3storage
<zodb_db main>
    # Main S3Storage database
    mount-point /
    connection-class Products.s3storage.connection.StoragePerConnection
    <s3storage>
      aws_access_key_id 12345ABC...
      aws_secret_access_key abc123.....
      bucket_name mys3bucket
      memcache_servers 127.0.0.1:12345
    </s3storage>
</zodb_db>


Current Status
--------------
Dog slow ;-)
Far too many writes to s3. Will be interesting to see what performance you
get from ec2 when they have some more beta keys


Design ideas - not completely current!
------------

s3storage is an experiment. Amazon's S3 storage service provides an eventual 
consistancy guarentee, but propogation delays mean that one must be careful 
when trying to cram transactional semantics on top of the model. That said, it
has the potential (along with the elastic compute cloud) to provide a massively
scalable object system.

Benefits of S3:

 * Simple model, it's just a massive hash table
 * Cheap. $0.15 per GB per month
 * Scalable
 * Reliable (eventually)

Drawbacks:

 * Indeterminate propogation time (the time between when a write occurs and
   a read on the resource can be relied upon)
 * No built in transaction support
 * No renaming (so we can't reuse Directory Storage)

So that leaves EC2 (the elastic compute cloud) to fill in the gaps.

Benefits of EC2:

 * Powerful virtual machines on demand
 * Cheap. $0.10 per vm per hour
 * It's Linux (pretty much any distribution you want)

Drawbacks:

 * Individual VMs are not reliable, this runs on commodity hardware, expect them
   to fail occaisionally

Bandwidth for both services is charge at $0.20 per GB, but only outside the AWS
system (so it's free between S3 and EC2)

So the challenge is to build a scalable zope system that leverages the S3's
scalability while making up for it's shortcomings with EC2.

Vision
======

Build an S3 storage backend that can scale to many readers (lots of zope
clients) but delegate locking to a known system such as zeo or postgres
for writes. Realtime systems are not the target here, think content and asset
management.

Model
-----

S3 provides no locking. So this must be provided for in EC2.

After a commit  an object while spend an indeterminate time before it becomes
reliably readable. Until that time has passed the view is not reliable. For
sake of argument lets say Tp = 5 minutes. This means that the last five minutes
of object state must be manged through a transactional system (think zeo or
postgres). There's no use trying to solve the read only case if the write case
is the bottleneck...

This gives us three pools of object revisions:

 * Uncommitted pool.

 * Committed, but not to be relied upon. The centrally accounted pool.

 * Propogated and relied upon. The distributed pool.

And two distribution methods:

 * Scalable but limited S3

Performance considerations
--------------------------

Read performance encourages us to opt for the distributed path. But this incurs
a time gap that is potentially disasterous for write performance.
Potentially we get a conflict error if any object is updated within Tp.

TODO: Understand commit retries. Could selected transactions be upgraded to a
more current view? Must avoid this central hit on every read.


Possible approaches to scalability
----------------------------------

 * Data partitioning. i.e. spread load between several locking servers. e.g.
   folder A on one server, folder B on another. Complicated to manage. Though
   it would be necessary for very large systems.

 * Something like memcached? Potentially simpler to manage.

 * Or Amazon SQS for locking? Use a queue as a lock, make oids prefixed by a
   client id (16 bits) followed by 48 bits of client increasing

Reliability considerations
--------------------------

 * EC2 is not reliable. We must consider the possibility transaction manager
   node failure. This should not be disasterous. As S3 has a guarentee of
   eventual consistancy so potentially no more than ~Tp down time would be
   necessary on node failure, and then only for writes. The storage scheme must
   prepare for this.

   NOTE: this assumes a guarantee of a successful write eventually succeeding.
   I _think_ S3 gives this guarantee.

 * But overhead of disaster preparedness must not be so onerous as to overly
   limit the common case.


Filesystem Structure
====================

 * S3 is a giant hash table, not a hierarchical filesystem

 * S3 provides an efficient list operation. Listing a bucket can be filtered by
   prefix, limited by max-results and also have results rolled up with a
   delimiter (such as '/'). A marker can be provided to list only results that
   are alphabetically after a particular key. List results are always returned
   in alphabetical order.

 * S3 is atomic only for single file puts.

 * Only existance is certain

 * Existing filestorage strategies do not map well

 * Three places to store information, the key, the file contents and
   metadata. Only the key is returned on a list. (actually some s3 system
   metatdata is returned here too). keys must be UTF-8 and up to 1024 bytes.
   If keys contains data you don't already know then two requests are required
   to read the contents. 

Transaction Log
---------------

 * Only a single file operations are atomic. They either succeed or fail.
   Latest timestamp wins for operations on the same resource.

On a transaction commit a transaction log is written such as

    type:transaction,serial:<serial repr>,prev:<prev serial repr>,tid:<tid repr>,ltid:<prev tid repr>

It contains a list of modified oids

Precondition:
all pickle writes must have successfully completed before this file is written

The prev: information is there so that readers can be certain they are seeing a
complete list (only existance is certain)

Efficiency of reading the transaction log
-----------------------------------------

We have a design decision here. We can choose to efficiently read from the end
of the filelist or from a certain point in the list depending on the
representation format used for tids, i.e. if tid=1 ends up alphabetically lower
or higher than tid=2.

(Note we could use another index: field but it would add complication)

Records
-------

 * A record is the state of an object at a particular transaction

On a store a record is written with a key

    type:record,oid:<oid repr>,serial:<serial repr>

Note that records can exist for data in aborted or in progress transactions

Indexing
--------

We want to avoid having to read through the entire transaction log in order
to find out

  * The record for an oid currently or at a particular transaction
    loadBefore, load

  this should be implemented as a list operation. So we write a 0 byte file

    type:index_commit,oid:<oid repr>,serial:<serial repr>,prev:<prev serial repr>,tid:<tid repr>,ltid:<ltid repr>

  and the serial for a tid

    type:index_tid,tid:<tid repr>,serial:<serial repr>,prev:<prev serial repr>,ltid<ltid repr>

So this means that sequence must be reverse alphabetically sorted
  
These are starting to look very like rdf data structures
