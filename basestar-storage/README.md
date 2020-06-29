# Storage Layer

## Storage Interface

The storage layer is responsible for:

- Persisting schema-typed data
- Maintaining indexes
- Enforcing the level(s) of consistency advertised in StorageTraits
- Converting query expressions into performant queries against the underlying storage

Links, references, inheritance and authentication are not handled by the storage layer, they are handled by the database
layer (although it maybe makes sense to optionally push some of this down where the storage engine
can support it, e.g. SQL).

### Required methods

#### read(id)

Required, returns the latest version of the object with this id

#### read(id, version)

If version history is not implemented/enabled, the storage engine should perform a read, and return null if the
current version doesn't match the requested version.

### Optional methods

#### query(expression, sort)

The storage engine should return a list of Pager sources, it is the responsibility of the caller to merge these Pager
sources. A storage engine should support only the subset of queries that can be implemented optimally.

#### aggregate(expression)

As per query, but for aggregating view types.

#### create(id, data)

#### update(id, version, data)

#### delete(id, version)

### StorageTraits

An implementation should advertise the consistency level it supports for each of:

- Object read/write
- History read/write
- Polymorphic object read/write
- Single-value index read/write
- Multi-value index read/write
- Multi-object read/write

Consistency may be one of:

- `ATOMIC` a set of writes with this consistency succeed or fail as a group and are read-isolated
- `QUORUM` a set of writes with this consistency succeed when a quorum as defined by the storage engine have committed
- `EVENTUAL` a set of writes with this consistency will eventually succeed if the write request returns a success response
- `ASYNC` a set of writes with this consistency will be attempted best-effort
- `NONE` no guarantees are made about this write, for internal/special case use only.

### Events

Storage engines should indicate to the caller (typically a co-ordinating database instance) how to emit
events based on changes. In the ideal case, the storage implementation will emit its own events using
a reliable mechanism defined by the underlying implementation (e.g. DynamoDB streams).

### Delegating storage

The `DelegatingStorage` interface may be used to mix-and-match different storage implementations
for different types or different access patterns.

### Queries

There are general utilities in `io.basestar.expression` for deconstructing/splitting/introspecting and walking
expressions. Wherever possible, storage engines should push queries down to the underling implementation rather
then doing in-memory filtering.

Different engines will have different constraints on what kind of queries and what kind of indexes are allowed,
eventually this information should be captured into StorageTraits or similar, so that query and index
validation can be done more consistently and earlier.

The abstract class `PartitionedStorage` contains a basic control-inverted implementation of hash-range storage,
with query re-structuring into disjunction-of-conjunctions form, index matching, etc. This is a suitable base
class for DynamoDB, Cassandra etc implementations.

### Storage Test Suite

...

## Stash Interface

Stash is a lower-level storage-like interface, used primarily for offloading 'oversize' or otherwise invalid
records from constrained storage implementations, e.g. DynamoDB records over a certain size can be offloaded
to S3 (except for those properties required for index queries.)


## Examples

### Mock/ephemeral primary storage

```java
final MemoryStorage primaryStorage = MemoryStorage.builder().build();

final DatabaseServer primaryDatabase = new DatabaseServer(namespace, primaryStorage);

// Expose primary database on port 8080
final API api = new DatabaseAPI(authenticator, primaryDatabase);
final UndertowConnector connector = new UndertowConnector(api, "localhost", 8080);
connector.start();

```


### DynamoDB single-table primary storage, S3 oversize storage, replicated to Elasticsearch

**Note:** this establishes replication using in-process event polling, running on the same container as
the API server, however the event wiring could equally be achieved using a lambda DynamoDB stream handler
attached to the primary database table(s) rather than an SNS emitter. Replication could also
be handled in a Lambda listening to the queue instead (using the same codebase).

```java
final S3AsyncClient s3 = S3AsyncClient.builder().build();
final SnsAsyncClient sns = SnsAsyncClient.builder().build();
final SqsAsyncClient sqs = SqsAsyncClient.builder().build();
final DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder().build();

// Low-level S3 storage for large DDB records
final S3Stash storageOversize = S3Stash.builder()
    .setClient(s3).setBucket(storageOversizeBucket).build();

// Low-level S3 storage for large SQS/SNS events
final S3Stash eventOversize = S3Stash.builder()
    .setClient(s3).setBucket(eventOversizeBucket).build();

// Emit events to SNS
final Emitter emitter = SNSEmitter.builder()
    .setClient(sns).setTopicArn(topicArn).set.build();

// Receive events from SQS (assumed that queue is subscribed to topic)
final Receiver receiver = SQSReceiver.builder()
    .setClient(sqs).setQueueUrl(queueUrl).build();

// Primary storage
final DynamoDBRouting ddbRouting = new DynamoDBRouting.SingleTable(dynamoDBPrefix);

final Storage primaryStorage = DynamoDBStorage.builder()
    .setClient(ddb).setRouting(ddbRouting)
    .setOversizeStash(storageOversize).build();

// Secondary storage (configured without history, and with default search mappings)
final ElasticsearchRouting esRouting = ElasticsearchRouting.Simple.builder()
    .setMode(ElasticsearchRouting.Mode.OBJECT_ONLY)
    .setMappingsFactory(new Mappings.Factory.Default())
    .build();

final Storage secondaryStorage = ElasticsearchStorage.builder()
    .setClient(es).setRouting(esRouting).build();

// Primary database server
final DatabaseServer primaryDatabase = new DatabaseServer(namespace, primaryStorage, emitter);

// Secondary database server
final DatabaseServer secondaryDatabase = new DatabaseServer(namespace, secondaryStorage);

// Replicator from primary -> secondary (authenticating as superuser)
final Replicator replicator = new Replicator(Caller.SUPER, secondaryDatabase);

// Start pumping events to the replicator
final Pump pump = Pump.create(receiver, replicator);
pump.start();

// Expose primary database on port 8080
final API api = new DatabaseAPI(authenticator, primaryDatabase);
final UndertowConnector connector = new UndertowConnector(api, "localhost", 8080);
connector.start();

```

## Layered storage/data-branching

Data-branching is implemented in the general case using LayeredStorage, this storage engine has a primary (overlay) and
secondary (base) storage engine, the primary and secondary storage engine may be of different types.

In the special case, where layering storage may be implemented more efficiently when the underlying engines have
awareness of each other and the layering, a custom implementation can be provided (how?). An example of where awareness
may lead to better performance is Elasticsearch, where multi-index searches could be used to provide the overlay feature.

Multiple LayeredStorage instances may be composed to represent complex chains of branching, with the caveat that
performance degradation may occur at deeper levels, depending on the layering mechanism, e.g. the default LayeredStorage
implementation will perform at least n times more work than the simple case where n is the number of layers.

Layered storage from the point-of-view of the caller should appear as if the base storage was duplicated, in terms of
reads, queries and writes, but only a minimal amount of data (the difference) should be stored.

A general LayeredStorage implementation for partitioned storage (e.g. dynamodb, cassandra) works as follows:

This assumes that at a given branch point, there is one writer which can select behaviour depending on which branch
it is acting for, and no other writers can change the data in the storage.

#### create

If writing to the primary, the instance is created in the primary storage only.

If writing to the secondary, the instance is created in the secondary storage, and a tombstone is created on all
indexes that cover the instance in the primary storage.

#### delete

If writing to the primary, a tombstone record is created in the primary storage, and all covering indexes.

If writing to the secondary, a tombstone record is created in the secondary storage and all covering indexes.

#### update

If writing to the primary, the instance is updated in the primary storage new index records are created as normal, and
a tombstone record is created in the primary storage for all prior covering indexes that are no longer covering indexes.

If writing to the secondary, the instance is updated in the secondary storage, new index records are created as normal,
and a tombstone record is created in the secondary storage for all prior covering indexes that are no longer covering
indexes. Tombstone records are also created in the primary storage for all newly covering indexes,
unless non-tombstone records exist for the same key, and the original value from the secondary storage is written to the
primary storage.

#### read

The read request is executed on the primary and secondary storage engines in parallel, the response from the primary is
returned if present, null is returned if a tombstone is present, otherwise the response from the secondary is returned.

#### query

The query ie executed on the primary and the secondary storage engines in parallel, since both queries have the same
sort term(s), and since tombstones will have been placed on top of values that changed, it is possible to remove duplicates
and overwritten tombstones by comparing only the values at the heads of the paging sources. This kind of de-duplication is
already done for disjunctive queries on hash-range stores, the only difference is the introduction of tombstones to cover
values that had their keys changed.

### Example

Assume the following record is written to storage X:

<table>
<tr><td>id</td><td>a</td></tr>
<tr><td>version</td><td>1</td></tr>
<tr><td>name</td><td>matt</td></tr>
</table>

Storage X now looks like:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>a</td><td>1</td><td>matt</td></tr>
</table>

Now we'll create a LayeredStorage with X as secondary and Y as primary. Let's write to the primary (overlay):

<table>
<tr><td>id</td><td>b</td></tr>
<tr><td>version</td><td>1</td></tr>
<tr><td>name</td><td>sandy</td></tr>
</table>

Storage X is unchanged, storage Y now looks like:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>b</td><td>1</td><td>sandy</td></tr>
</table>

The layered storage view looks like:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>a</td><td>1</td><td>matt</td></tr>
<tr><td>b</td><td>1</td><td>sandy</td></tr>
</table>

Let's write to the secondary (base) now:

<table>
<tr><td>id</td><td>c</td></tr>
<tr><td>version</td><td>1</td></tr>
<tr><td>name</td><td>mark</td></tr>
</table>

Storage X now looks like this:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>a</td><td>1</td><td>matt</td></tr>
<tr><td>c</td><td>1</td><td>mark</td></tr>
</table>

Storage Y looks like this:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>b</td><td>1</td><td>sandy</td></tr>
<tr><td>c*</td><td>1</td><td>mark</td></tr>
</table>

The c* record is a tombstone record, reads on the primary side of the storage will receive both values for c, and treat
the tombstone record as an indicator that the record does not exist in this side of the branch.

If we now edit the first record:

<table>
<tr><td>id</td><td>a</td></tr>
<tr><td>version</td><td>2</td></tr>
<tr><td>name</td><td>sean</td></tr>
</table>

Storage X now looks like this:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>a</td><td>2</td><td>sean</td></tr>
<tr><td>c</td><td>1</td><td>mark</td></tr>
</table>

Storage Y now looks like this:

<table>
<tr><td>id</td><td>version</td><td>name</td></tr>
<tr><td>b</td><td>1</td><td>sandy</td></tr>
<tr><td>c*</td><td>1</td><td>mark</td></tr>
<tr><td>a</td><td>1</td><td>matt</td></tr>
</table>

If there were an index on the `name` field, then the same logic applies to the primary key of the index instead.

