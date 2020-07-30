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
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class SubscriptionPublishEvent implements ObjectEvent {

    private Name schema;

    private String id;

    private Change.Event event;

    private Map<String, Object> before;

    private Map<String, Object> after;

    private Subscription subscription;

    public static SubscriptionPublishEvent of(final Name schema, final String id, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after, final Subscription subscription) {

        return new SubscriptionPublishEvent().setSchema(schema).setId(id).setEvent(event).setBefore(before).setAfter(after).setSubscription(subscription);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}
