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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompletableFutures {

    private CompletableFutures() {

    }

    public static <T> CompletableFuture<T> completedExceptionally(final Throwable err) {

        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(err);
        return future;
    }

    public static <T> CompletableFuture<Stream<T>> allOf(final Stream<? extends CompletableFuture<T>> futures) {

        final List<CompletableFuture<T>> list = futures.collect(Collectors.toList());
        return allOf(list).thenApply(List::stream);
    }

    public static <T> CompletableFuture<List<T>> allOf(final List<? extends CompletableFuture<T>> futures) {

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.stream().map(v -> v.getNow(null)).collect(Collectors.toList()));
    }

    public static <K, T> CompletableFuture<Map<K, T>> allOf(final Map<? extends K, ? extends CompletableFuture<T>> futures) {

        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> Immutable.transformValues(futures, (k, v) -> v.getNow(null)));
    }

    public static <T> CompletableFuture<T> allOf(final T identity, final BinaryOperator<T> accumulator, final List<CompletableFuture<T>> futures) {

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.stream().map(v -> v.getNow(null)).reduce(identity, accumulator));
    }

    public static <T> CompletableFuture<Optional<T>> allOf(final BinaryOperator<T> accumulator, final List<CompletableFuture<T>> futures) {

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.stream().map(v -> v.getNow(null)).reduce(accumulator));
    }
}
