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
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Handlers<T> {

    @Data
    public static class Mapping<T, E extends Event> {

        private final Class<E> event;

        private final UnboundHandler<T, E> handler;
    }

    public interface UnboundHandler<T, E extends Event> {

        CompletableFuture<?> handle(T self, E event);
    }

    private final List<Mapping<T, ?>> mappings;

    public Handlers(final List<Mapping<T, ?>> mappings) {

        this.mappings = ImmutableList.copyOf(mappings);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<?> handle(final T self, final Event event) {

        final Collection<UnboundHandler<T, ?>> handlers = mappings.stream()
                .filter(e -> e.getEvent().equals(event.getClass()))
                .map(Mapping::getHandler)
                .collect(Collectors.toList());
        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for(final UnboundHandler<T, ?> handler : handlers) {
            futures.add(((UnboundHandler<T, Event>)handler).handle(self, event));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    public static class Builder<T> {

        private final List<Mapping<T, ?>> mappings = new ArrayList<>();

        public <E extends Event> Builder<T> on(final Class<E> event, final UnboundHandler<T, E> handler) {

            mappings.add(new Mapping<>(event, handler));
            return this;
        }

        public <E extends Event> Builder<T> on(final Class<E> event, final Handler<E> handler) {

            mappings.add(new Mapping<>(event, (ignored, e) -> handler.handle(e)));
            return this;
        }

        public Handlers<T> build() {

            return new Handlers<>(mappings);
        }
    }

    public static <T> Builder<T> builder() {

        return new Builder<>();
    }
}
