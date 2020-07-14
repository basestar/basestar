package io.basestar.util;

/*-
 * #%L
 * basestar-core
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

import java.util.concurrent.CompletableFuture;

public class CompletableFutures {

//    static <I, O> CompletableFuture<O> mergeFutures(final Stream<? extends CompletableFuture<? extends BatchResponse>> responses) {
//
//        final List<CompletableFuture<? extends BatchResponse>> futures = responses.collect(Collectors.toList());
//
//        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
//                .thenApply(ignored -> BatchResponse.merge(futures.stream()
//                        .map(v -> v.getNow(null))
//                        .map(v -> v == null ? BatchResponse.empty() : v)));
//    }

    public static <T> CompletableFuture<T> completedExceptionally(final Throwable err) {

        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(err);
        return future;
    }
}
