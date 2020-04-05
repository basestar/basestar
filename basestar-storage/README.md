# Storage Layer

## Storage Interface

The storage layer is responsible for:

- Persisting schema-typed data
- Maintaining indexes
- Enforcing the level(s) of consistency advertised in StorageTraits
- Converting query expressions into performant queries against the underlying storage

Links, references and authentication are not handled by the storage layer, they are handled by the database
layer (although it maybe makes sense to optionally push some of this down where the storage engine
can support it, e.g. SQL).

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

```
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

```
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
