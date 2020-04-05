package io.basestar.storage;

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
