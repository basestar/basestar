package io.basestar.event;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public interface Emitter {

    static Emitter skip() {

        return new Skip();
    }

    default CompletableFuture<?> emit(final Event event) {

        return emit(Collections.singletonList(event));
    }

    CompletableFuture<?> emit(Collection<? extends Event> event);

    class Skip implements Emitter {

        @Override
        public CompletableFuture<?> emit(final Collection<? extends Event> event) {

            return CompletableFuture.completedFuture(null);
        }
    }

    class Multi implements Emitter {

        private final ImmutableList<Emitter> emitters;

        public Multi(final Collection<? extends Emitter> emitters) {

            this.emitters = ImmutableList.copyOf(emitters);
        }

        @Override
        public CompletableFuture<?> emit(final Collection<? extends Event> events) {

            return CompletableFuture.allOf(emitters.stream().map(v -> v.emit(events))
                    .toArray(CompletableFuture<?>[]::new));
        }
    }

//    interface OversizeHandler {
//
//        CompletableFuture<String> write(String id, byte[] data);
//
//        static OversizeHandler fail() {
//
//            return Fail.INSTANCE;
//        }
//
//        class Fail implements OversizeHandler {
//
//            public static Fail INSTANCE = new Fail();
//
//            @Override
//            public CompletableFuture<String> write(final String id, final byte[] data) {
//
//                throw new UnsupportedOperationException();
//            }
//        }
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
//    }
}
