## DynamoDB Storage Engine

### Features

- Atomic object, history, and index read/write (up to 25 items per transaction)
- Large object support (via pluggable stash, e.g. io.basestar.storge.s3.S3Stash)
- Multi-value indexes (Async consistency only)
- Query pushdown for: EQ, LT, LTE, GT, GTE

### Caveats

- All indexes must have a partition key defined
- All queries must have EQ terms that match the entire partition
- Indexes used by a given query must have compatible sort keys