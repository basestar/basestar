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

import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Loopback implements Emitter, Receiver {

    private static final int DEFAULT_BATCH_SIZE = 50;

    private final ConcurrentLinkedQueue<EventWithMeta> queue = new ConcurrentLinkedQueue<>();

    private final int batchSize;

    public Loopback() {

        this(DEFAULT_BATCH_SIZE);
    }

    public Loopback(final int batchSize) {

        this.batchSize = batchSize;
    }

    @Override
    public CompletableFuture<?> emit(final Collection<? extends Event> events, final Map<String, String> meta) {

        events.forEach(e -> queue.add(new EventWithMeta(e, meta)));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> receive(final Handler<Event> handler) {

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i != batchSize; ++i) {
            final EventWithMeta event = queue.poll();
            if (event != null) {
                futures.add(handler.handle(event.getEvent(), event.getMeta()));
            } else {
                break;
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.size());
    }

    @Data
    private static class EventWithMeta {

        private final Event event;

        private final Map<String, String> meta;
    }
}
