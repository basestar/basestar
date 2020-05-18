package io.basestar.util;

import java.util.concurrent.CompletableFuture;

public class CompletableFutures {

    public static <T> CompletableFuture<T> completedExceptionally(final Throwable err) {

        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(err);
        return future;
    }
}
