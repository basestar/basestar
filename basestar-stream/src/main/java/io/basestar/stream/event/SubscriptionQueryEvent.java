package io.basestar.stream.event;

/*-
 * #%L
 * basestar-stream
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

import io.basestar.database.event.ObjectEvent;
import io.basestar.event.Event;
import io.basestar.stream.Change;
import io.basestar.stream.Subscription;
import io.basestar.util.PagingToken;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@Accessors(chain = true)
public class SubscriptionQueryEvent implements ObjectEvent {

    private String schema;

    private String id;

    private Change.Event event;

    private Long before;

    private Long after;

    private Set<Subscription.Key> keys;

    private PagingToken paging;

    public static SubscriptionQueryEvent of(final String schema, final String id, final Change.Event event, final Long before, final Long after, final Set<Subscription.Key> keys) {

        return of(schema, id, event, before, after, keys, null);
    }

    public static SubscriptionQueryEvent of(final String schema, final String id, final Change.Event event, final Long before, final Long after, final Set<Subscription.Key> keys, final PagingToken paging) {

        return new SubscriptionQueryEvent().setSchema(schema).setId(id).setEvent(event).setBefore(before).setAfter(after).setKeys(keys).setPaging(paging);
    }

    public SubscriptionQueryEvent withPaging(final PagingToken paging) {

        return of(schema, id, event, before, after, keys, paging);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}
