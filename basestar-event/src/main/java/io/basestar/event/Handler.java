package io.basestar.event;

import java.util.concurrent.CompletableFuture;

public interface Handler<E extends Event> {

    CompletableFuture<?> handle(E event);

}
