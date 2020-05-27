package io.basestar.stream;

import java.util.concurrent.CompletableFuture;

public interface Publisher {

    CompletableFuture<?> publish(String sub, String channel, Change change);
}
