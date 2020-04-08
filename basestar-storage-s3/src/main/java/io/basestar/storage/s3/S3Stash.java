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

import io.basestar.storage.Stash;
import lombok.Setter;
import lombok.experimental.Accessors;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.concurrent.CompletableFuture;

public class S3Stash implements Stash {

    private final S3AsyncClient client;

    private final String bucket;

    private final String prefix;

    private S3Stash(final Builder builder) {

        this.client = builder.client;
        this.bucket = builder.bucket;
        this.prefix = builder.prefix;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private S3AsyncClient client;

        private String bucket;

        private String prefix;

        public S3Stash build() {

            return new S3Stash(this);
        }
    }

    @Override
    public CompletableFuture<String> write(final String id, final byte[] data) {

        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket).key(key(id)).build();
        return client.putObject(request, AsyncRequestBody.fromBytes(data))
                .thenApply(ignored -> id);
    }

    @Override
    public CompletableFuture<byte[]> read(final String ref) {

        final GetObjectRequest get = GetObjectRequest.builder()
                .bucket(bucket).key(key(ref)).build();

        return client.getObject(get, AsyncResponseTransformer.toBytes())
                .thenApply(BytesWrapper::asByteArray);
    }

    @Override
    public CompletableFuture<?> delete(final String ref) {

        final DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucket).key(key(ref)).build();
        return client.deleteObject(request);
    }

//    @Override
//    protected CompletableFuture<String> write(final String key, final byte[] data) {
//
//        final PutObjectRequest request = PutObjectRequest.builder()
//                .bucket(bucket).key(key(key)).build();
//        return s3.putObject(request, AsyncRequestBody.fromBytes(data))
//                .thenApply(ignored -> key);
//    }
//
//    @Override
//    protected CompletableFuture<byte[]> read(final String key) {
//
//        final GetObjectRequest get = GetObjectRequest.builder()
//                .bucket(bucket).key(key(key)).build();
//
//        return s3.getObject(get, AsyncResponseTransformer.toBytes())
//                .thenApply(BytesWrapper::asByteArray);
//    }
//
//    @Override
//    protected CompletableFuture<?> delete(final String key) {
//
//        final DeleteObjectRequest request = DeleteObjectRequest.builder()
//                .bucket(bucket).key(key(key)).build();
//        return s3.deleteObject(request);
//    }

    private String key(final String ref) {

        return prefix + ref;
    }
}
