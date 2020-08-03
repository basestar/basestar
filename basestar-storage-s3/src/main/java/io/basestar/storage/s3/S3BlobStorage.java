package io.basestar.storage.s3;

/*-
 * #%L
 * basestar-storage-s3
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

import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.util.Name;
import io.basestar.storage.Versioning;
import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class S3BlobStorage implements Storage.WithoutWriteIndex, Storage.WithoutWriteHistory, Storage.WithoutQuery, Storage.WithoutExpand {

    private final S3AsyncClient client;

    private final S3BlobStrategy strategy;

    private S3BlobStorage(final Builder builder) {

        this.client = builder.client;
        this.strategy = builder.strategy;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private S3AsyncClient client;

        private S3BlobStrategy strategy;

        public S3BlobStorage build() {

            return new S3BlobStorage(this);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final String bucket = strategy.objectBucket(schema);
        final String key = objectKey(schema, id);
        return readObjectImpl(bucket, key);
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        final String bucket = strategy.historyBucket(schema);
        final String key = historyKey(schema, id, version);
        return readObjectImpl(bucket, key);
    }

    private CompletableFuture<Map<String, Object>> readObjectImpl(final String bucket, final String key) {

        return readImpl(bucket, key)
                .thenApply(bytes -> {
                    if(bytes == null || bytes.length == 0) {
                        // Tombstone record
                        return null;
                    } else {
                        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                             final DataInputStream dis = new DataInputStream(bais)) {
                            return ObjectSchema.deserialize(dis);
                        } catch (final IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
    }

    private CompletableFuture<byte[]> readImpl(final String bucket, final String key) {

        final GetObjectRequest get = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return client.getObject(get, AsyncResponseTransformer.toBytes())
                .thenApply(BytesWrapper::asByteArray)
                .exceptionally(t -> {
                    if(t.getCause() instanceof NoSuchKeyException) {
                        return null;
                    } else if(t.getCause() instanceof RuntimeException) {
                        throw (RuntimeException)t.getCause();
                    } else {
                        throw new IllegalStateException(t.getCause());
                    }
                });
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction() {

            final List<Supplier<CompletableFuture<BatchResponse>>> steps = new ArrayList<>();

            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                steps.add(() -> writeObject(schema, id, after)
                        .thenApply(v -> BatchResponse.single(schema.getQualifiedName(), after)));
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                steps.add(() -> writeObject(schema, id, after)
                        .thenApply(v -> BatchResponse.single(schema.getQualifiedName(), after)));
                return this;
            }

            private CompletableFuture<String> writeObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                final byte[] object = encode(schema, after);
                final String bucket = strategy.objectBucket(schema);
                final String key = objectKey(schema, id);
                return writeImpl(bucket, key, object);
            }

            private CompletableFuture<String> writeHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                final byte[] object = encode(schema, after);
                final String bucket = strategy.historyBucket(schema);
                final String key = historyKey(schema, id, version);
                return writeImpl(bucket, key, object);
            }

            private byte[] encode(final ObjectSchema schema, final Map<String, Object> object) {

                try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final DataOutputStream dos = new DataOutputStream(baos)) {
                    schema.serialize(object, dos);
                    return baos.toByteArray();
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            private CompletableFuture<String> writeImpl(final String bucket, final String key, final byte[] bytes) {

                final PutObjectRequest put = PutObjectRequest.builder()
                        .bucket(bucket).key(key).build();
                return client.putObject(put, AsyncRequestBody.fromBytes(bytes))
                        .thenApply(PutObjectResponse::versionId);
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final String bucket = strategy.objectBucket(schema);
                final String key = objectKey(schema, id);
                final DeleteObjectRequest request = DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                steps.add(() -> client.deleteObject(request).thenApply(v -> BatchResponse.empty()));
                return this;
            }

//            @Override
//            public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {
//
//                steps.add(() -> writeHistory(schema, id, version, after)
//                        .thenApply(v -> BatchResponse.empty()));
//                return this;
//            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return BatchResponse.mergeFutures(steps.stream().map(Supplier::get));
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema objectSchema) {

        return EventStrategy.EMIT;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return S3BlobStorageTraits.INSTANCE;
    }

    private String objectKey(final ObjectSchema schema, final String id) {

        final String prefix = strategy.objectPrefix(schema);
        return prefix + id;
    }

    private String historyKey(final ObjectSchema schema, final String id, final long version) {

        final String prefix = strategy.historyPrefix(schema);
        return prefix + id + "/" + version;
    }
}
