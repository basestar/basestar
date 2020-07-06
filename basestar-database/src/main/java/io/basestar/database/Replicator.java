package io.basestar.database;

/*-
 * #%L
 * basestar-database
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

import io.basestar.auth.Caller;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.database.options.CreateOptions;
import io.basestar.database.options.DeleteOptions;
import io.basestar.database.options.UpdateOptions;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Replicator implements Handler<Event> {

    private static final Handlers<Replicator> HANDLERS = Handlers.<Replicator>builder()
            .on(ObjectCreatedEvent.class, Replicator::onCreated)
            .on(ObjectUpdatedEvent.class, Replicator::onUpdated)
            .on(ObjectDeletedEvent.class, Replicator::onDeleted)
            .build();

    private final Caller caller;

    private final Database target;

    public Replicator(final Caller caller, final Database target) {

        this.caller = caller;
        this.target = target;
    }

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> meta) {

        return HANDLERS.handle(this, event, meta);
    }

    private CompletableFuture<?> onCreated(final ObjectCreatedEvent event) {

        final CreateOptions options = CreateOptions.builder()
                .schema(event.getSchema())
                .id(event.getId())
                .build();
        return target.create(caller, options);
    }

    private CompletableFuture<?> onUpdated(final ObjectUpdatedEvent event) {

        final UpdateOptions options = UpdateOptions.builder()
                .schema(event.getSchema())
                .id(event.getId())
                .data(event.getAfter())
                .version(event.getVersion())
                .mode(UpdateOptions.Mode.CREATE)
                .build();
        return target.update(caller, options);
    }

    private CompletableFuture<?> onDeleted(final ObjectDeletedEvent event) {

        final DeleteOptions options = DeleteOptions.builder()
                .schema(event.getSchema())
                .id(event.getId())
                .version(event.getVersion())
                .build();
        return target.delete(caller, options);
    }
}
