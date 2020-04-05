package io.basestar.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Loopback implements Emitter, Receiver {

    private static final int DEFAULT_BATCH_SIZE = 50;

    private final ConcurrentLinkedQueue<Event> queue = new ConcurrentLinkedQueue<>();

    private final int batchSize;

    //private final Multimap<String, Handler<Event>> handlers = HashMultimap.create();

    public Loopback() {

        this(DEFAULT_BATCH_SIZE);
    }

    public Loopback(final int batchSize) {

        this.batchSize = batchSize;
    }

    @Override
    public CompletableFuture<?> emit(final Collection<? extends Event> events) {

        queue.addAll(events);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> receive(final Handler<Event> handler) {

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for(int i = 0; i != batchSize; ++i) {
            final Event event = queue.poll();
            if(event != null) {
                futures.add(handler.handle(event));
            } else {
                break;
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.size());
    }
}
