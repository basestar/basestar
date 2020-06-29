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

Data-branching is implemented in the general case using LayeredStorage, this storage engine has a baseline
and left and right storage engines, baseline, left and right storage engines may be of different types.

In the special case, where layering storage may be implemented more efficiently when the underlying engines have
awareness of each other and the layering, a custom implementation can be provided. An example of where awareness
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

Instances are created on left/right branch storage depending on the request

#### delete

If the instance exists in the baseline, a tombstone is written to the selected storage branch, else it is deleted
directly.

#### update

The instance is written to the selected storage branch, if the value existed in the baseline storage with different
index keys, then tombstone records are written to those index keys (assuming that no current records exist for those
index keys).

#### read

The read request is executed on the selected branch and the baseline engines in parallel, the response from the left/right branch is
returned if present, null is returned if a tombstone is present, otherwise the response from the baseline is returned.

#### query

The query ie executed on the selected branch and the baseline engines in parallel, since both queries have the same
sort term(s), and since tombstones will have been placed on top of values that changed, it is possible to remove duplicates
and overwritten tombstones by comparing only the values at the heads of the paging sources. This kind of de-duplication is
already done for disjunctive queries on hash-range stores, the only difference is the introduction of tombstones to cover
values that had their keys changed.

### Example

Assume the following record is written to baseline storage, there is an index on `name`:

<table>
    <tr><td>id</td><td>a</td></tr>
    <tr><td>version</td><td>1</td></tr>
    <tr><td>name</td><td>matt</td></tr>
</table>

Baseline storage now looks like:

<table>
    <tr><td>id</td><td>version</td><td>name</td></tr>
    <tr><td>a</td><td>1</td><td>matt</td></tr>
</table>

Baseline index looks like:

<table>
    <tr><td>name</td><td>id</td><td>version</td></tr>
    <tr><td>matt</td><td>a</td><td>1</td></tr>
</table>

Now we'll create a LayeredStorage and write to the left side:

<table>
    <tr><td>id</td><td>b</td></tr>
    <tr><td>version</td><td>1</td></tr>
    <tr><td>name</td><td>sandy</td></tr>
</table>

Baseline is unchanged, left storage now looks like:

<table>
    <tr><td>id</td><td>version</td><td>name</td></tr>
    <tr><td>b</td><td>1</td><td>sandy</td></tr>
</table>

Let's write to the right side:

<table>
    <tr><td>id</td><td>c</td></tr>
    <tr><td>version</td><td>1</td></tr>
    <tr><td>name</td><td>mark</td></tr>
</table>

If we now edit the first record on the left side:

<table>
    <tr><td>id</td><td>a</td></tr>
    <tr><td>version</td><td>2</td></tr>
    <tr><td>name</td><td>sean</td></tr>
</table>

Left storage now looks like this:

<table>
    <tr><td>id</td><td>version</td><td>name</td></tr>
    <tr><td>a</td><td>2</td><td>sean</td></tr>
</table>

Left storage name index looks like:

<table>
    <tr><td>name</td><td>id</td><td>version</td></tr>
    <tr><td>matt*</td><td>-</td><td>-</td></tr>
    <tr><td>sean</td><td>a</td><td>2</td></tr>
</table>

`matt*` is a tombstone record, when read in a query it covers the value from the baseline storage engine and stops it
from being returned.

The layered storage view from the left looks like:

<table>
    <tr><td>id</td><td>version</td><td>name</td></tr>
    <tr><td>a</td><td>2</td><td>sean</td></tr>
    <tr><td>b</td><td>1</td><td>sandy</td></tr>
    <tr><td>c</td><td>1</td><td>mark</td></tr>
</table>

And from the right it looks looks like:

<table>
    <tr><td>id</td><td>version</td><td>name</td></tr>
    <tr><td>a</td><td>2</td><td>matt</td></tr>
    <tr><td>b</td><td>1</td><td>sandy</td></tr>
    <tr><td>c</td><td>1</td><td>mark</td></tr>
</table>




