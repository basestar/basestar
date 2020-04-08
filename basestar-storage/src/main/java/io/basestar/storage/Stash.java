package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Stash {

    CompletableFuture<String> write(String key, byte[] data);

    default CompletableFuture<String> write(final byte[] data) {

        return write(UUID.randomUUID().toString(), data);
    }

    CompletableFuture<byte[]> read(String key);

    CompletableFuture<?> delete(String key);
}


//
//interface OversizeHandler {
//
//    CompletableFuture<byte[]> read(String ref);
//
//    CompletableFuture<?> delete(String ref);
//
//    static OversizeHandler fail() {
//
//        return Fail.INSTANCE;
//    }
//
//    class Fail implements OversizeHandler {
//
//        public static Fail INSTANCE = new Fail();
//
//        @Override
//        public CompletableFuture<byte[]> read(final String ref) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public CompletableFuture<?> delete(final String ref) {
//
//            throw new UnsupportedOperationException();
//        }
//    }
//
////        @Data
////        class S3 implements OversizeHandler {
////
////            private final S3AsyncClient s3;
////
////            private final String bucket;
////
////            private final String prefix;
////
////            @Override
////            public CompletableFuture<String> write(final String id, final byte[] data) {
////
////                final PutObjectRequest request = PutObjectRequest.builder()
////                        .bucket(bucket).key(prefix + id).build();
////                return s3.putObject(request, AsyncRequestBody.fromBytes(data))
////                        .thenApply(ignored -> id);
////            }
////        }
//}
