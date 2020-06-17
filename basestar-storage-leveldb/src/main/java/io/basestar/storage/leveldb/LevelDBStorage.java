package io.basestar.storage.leveldb;

/*-
 * #%L
 * basestar-storage-leveldb
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.io.BaseEncoding;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.storage.util.Pager;
import io.basestar.util.*;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class LevelDBStorage extends PartitionedStorage {

    private final DB db;

    private final Coordinator coordinator;

    @lombok.Builder(builderClassName = "Builder")
    LevelDBStorage(final DB db, final Coordinator coordinator) {

        this.db = Nullsafe.require(db);
        this.coordinator = Nullsafe.option(coordinator, Coordinator.Local::new);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return CompletableFuture.supplyAsync(() -> {
            final byte[] key = key(schema, id);
            final byte[] data = db.get(key);
            return fromBytes(data);
        });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return CompletableFuture.supplyAsync(() -> {
            final byte[] key = key(schema, id, version);
            final byte[] data = db.get(key);
            return fromBytes(data);
        });
    }

    @Override
    protected CompletableFuture<PagedList<Map<String, Object>>> queryIndex(final ObjectSchema schema, final Index index, final SatisfyResult satisfyResult, final Map<Path, Range<Object>> query, final List<Sort> sort, final int count, final PagingToken paging) {

        return CompletableFuture.supplyAsync(() -> {
            final List<Object> values = new ArrayList<>();
            values.addAll(satisfyResult.getPartition());
            values.addAll(satisfyResult.getSort());

            final byte[] key = key(schema, index, values);

            final DBIterator iter = db.iterator();
            if(paging != null) {
                iter.seek(paging.getValue());
            } else {
                iter.seek(key);
            }

            final List<Map<String, Object>> page = new ArrayList<>();
            for (int i = 0; i != count && iter.hasNext(); ++i) {
                final Map.Entry<byte[], byte[]> entry = iter.next();
                if (matches(entry.getKey(), key)) {
                    page.add(fromBytes(entry.getValue()));
                } else {
                    break;
                }
            }
            PagingToken newPaging = null;
            if(iter.hasNext()) {
                final Map.Entry<byte[], byte[]> entry = iter.next();
                if(matches(entry.getKey(), key)) {
                    newPaging = new PagingToken(entry.getKey());
                }
            }

            return new PagedList<>(page, newPaging);
        });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new Storage.ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new PartitionedStorage.WriteTransaction() {

            private final List<Consumer<WriteBatch>> writes = new ArrayList<>();

            private final Map<String, Consumer<DB>> checks = new HashMap<>();

            private final SortedMap<BatchResponse.Key, Map<String, Object>> changes = new TreeMap<>();

            @Override
            public Storage.WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                writeVersion(schema, id, 0L, after);
                return createIndexes(schema, id, after);
            }

            @Override
            public Storage.WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                final Long beforeVersion = before == null ? null : Instance.getVersion(before);
                writeVersion(schema, id, beforeVersion, after);
                return updateIndexes(schema, id, before, after);
            }

            @Override
            public Storage.WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final Long beforeVersion = before == null ? null : Instance.getVersion(before);
                final byte[] key = key(schema, id);
                if(beforeVersion != null) {
                    checkVersion(schema, id, beforeVersion);
                }
                writes.add(batch -> batch.delete(key));
                return deleteIndexes(schema, id, before);
            }

            private void writeVersion(final ObjectSchema schema, final String id, final Long beforeVersion, final Map<String, Object> after) {

                final long afterVersion = Nullsafe.require(Instance.getVersion(after));
                final byte[] key = key(schema, id);
                if(beforeVersion != null) {
                    checkVersion(schema, id, beforeVersion);
                }
                writes.add(batch -> {
                    final byte[] data = toBytes(schema, afterVersion, after);
                    batch.put(key, data);
                });
                createHistory(schema, id, afterVersion, after);
                changes.put(new BatchResponse.Key(schema.getName(), id, afterVersion), after);
            }

            private void checkVersion(final ObjectSchema schema, final String id, final long version) {

                final byte[] key = key(schema, id);
                check(key, db -> {
                    final byte[] data = db.get(key);
                    final long recordVersion = versionFromBytes(data);
                    if(recordVersion != version) {
                        if(version == 0L) {
                            throw new ObjectExistsException(schema.getName(), id);
                        } else {
                            throw new VersionMismatchException(schema.getName(), id, version);
                        }
                    }
                });
            }

            private void checkVersion(final ObjectSchema schema, final String id, final long version, final byte[] key) {

                check(key, db -> {
                    final byte[] data = db.get(key);
                    final long recordVersion = versionFromBytes(data);
                    if(recordVersion != version) {
                        throw new VersionMismatchException(schema.getName(), id, version);
                    }
                });
            }

            private void check(final byte[] key, final Consumer<DB> check) {

                final String lock = BaseEncoding.base64().encode(key);
                if(checks.containsKey(lock)) {
                    throw new IllegalStateException("Transaction cannot refer to same object twice");
                }
                checks.put(lock, check);
            }

            @Override
            public Storage.WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                final byte[] indexKey = key(schema, index, key, id);
                checkVersion(schema, id, version, indexKey);
                writes.add(batch -> {
                    final byte[] data = toBytes(schema, version, projection);
                    batch.put(indexKey, data);
                });
                return this;
            }

            @Override
            public Storage.WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                return createIndex(schema, index, id, version, key, projection);
            }

            @Override
            public Storage.WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                final byte[] indexKey = key(schema, index, key, id);
                checkVersion(schema, id, version, indexKey);
                writes.add(batch -> batch.delete(indexKey));
                return this;
            }

            @Override
            public Storage.WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                final byte[] key = key(schema, id, version);
                checkVersion(schema, id, 0L, key);
                writes.add(batch -> {
                    final byte[] data = toBytes(schema, version, after);
                    batch.put(key, data);
                });
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                return CompletableFuture.supplyAsync(() -> {
                    final Set<String> locks = checks.keySet();
                    try (final CloseableLock ignored = coordinator.lock(locks)) {

                        checks.forEach((k, check) -> {
                            check.accept(db);
                        });

                        final WriteBatch batch = db.createWriteBatch();

                        writes.forEach(write -> write.accept(batch));

                        db.write(batch);

                        return new BatchResponse.Basic(changes);
                    }
                });
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return LevelDBStorageTraits.INSTANCE;
    }

    private static byte[] key(final ObjectSchema schema, final String id) {

        return PartitionedStorage.binary(Arrays.asList(schema.getName(), null, id));
    }

    private static byte[] key(final ObjectSchema schema, final String id, final long version) {

        return PartitionedStorage.binary(Arrays.asList(schema.getName(), Reserved.PREFIX + Reserved.VERSION, id, invert(version)));
    }

    private static byte[] key(final ObjectSchema schema, final Index index, final Index.Key key, final String id) {

        final List<Object> all = new ArrayList<>(key.keys());
        if(!index.isUnique()) {
            all.add(id);
        }
        return key(schema, index, all);
    }

    private static byte[] key(final ObjectSchema schema, final Index index, final List<?> values) {

        final List<Object> all = new ArrayList<>();
        all.add(schema.getName());
        all.add(index.getName());
        all.addAll(values);
        return PartitionedStorage.binary(all);
    }

    private static long invert(final long version) {

        return Long.MAX_VALUE - version;
    }

    private static Map<String, Object> fromBytes(final byte[] data) {

        if(data == null) {
            return null;
        } else {
            try(final ByteArrayInputStream bais = new ByteArrayInputStream(data);
                final DataInputStream dis = new DataInputStream(bais)) {
                dis.readLong();
                return ObjectSchema.deserialize(dis);
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static byte[] toBytes(final ObjectSchema schema, final long version, final Map<String, Object> data) {

        if(data == null) {
            return null;
        } else {
            try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeLong(version);
                schema.serialize(data, dos);
                dos.flush();
                return baos.toByteArray();
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static long versionFromBytes(final byte[] data) {

        if(data == null) {
            return 0L;
        } else {
            try(final ByteArrayInputStream bais = new ByteArrayInputStream(data);
                final DataInputStream dis = new DataInputStream(bais)) {
                return dis.readLong();
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static boolean matches(final byte[] key, final byte[] match) {

        if(key.length < match.length) {
            return false;
        } else {
            for(int i = 0; i != match.length; ++i) {
                if(key[i] != match[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
