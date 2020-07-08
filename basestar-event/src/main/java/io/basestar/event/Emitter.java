package io.basestar.event;

/*-
 * #%L
 * basestar-event
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

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Emitter {

    static Emitter skip() {

        return new Skip();
    }

    default CompletableFuture<?> emit(final Event event) {

        return emit(event, Collections.emptyMap());
    }

    default CompletableFuture<?> emit(final Event event, final Map<String, String> meta) {

        return emit(Collections.singletonList(event), meta);
    }

    default CompletableFuture<?> emit(final Collection<? extends Event> events) {

        return emit(events, Collections.emptyMap());
    }

    CompletableFuture<?> emit(Collection<? extends Event> event, Map<String, String> meta);

    class Skip implements Emitter {

        @Override
        public CompletableFuture<?> emit(final Collection<? extends Event> event, final Map<String, String> meta) {

            return CompletableFuture.completedFuture(null);
        }
    }

    class Multi implements Emitter {

        private final ImmutableList<Emitter> emitters;

        public Multi(final Collection<? extends Emitter> emitters) {

            this.emitters = ImmutableList.copyOf(emitters);
        }

        @Override
        public CompletableFuture<?> emit(final Collection<? extends Event> events, final Map<String, String> meta) {

            return CompletableFuture.allOf(emitters.stream().map(v -> v.emit(events, meta))
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
