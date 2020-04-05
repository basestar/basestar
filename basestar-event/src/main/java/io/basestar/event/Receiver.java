package io.basestar.event;

import java.util.concurrent.CompletableFuture;

public interface Receiver {

    CompletableFuture<Integer> receive(Handler<Event> handler);

}
