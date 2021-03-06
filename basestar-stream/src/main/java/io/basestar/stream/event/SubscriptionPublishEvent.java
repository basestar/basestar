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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.basestar.event.Event;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.Change;
import io.basestar.stream.SubscriptionMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class SubscriptionPublishEvent implements Event {

    private Caller caller;

    private String sub;

    private String channel;

    private Change change;

    private SubscriptionMetadata metadata;

    public static SubscriptionPublishEvent of(final Caller caller, final String sub, final String channel, final Change change, final SubscriptionMetadata metadata) {

        return new SubscriptionPublishEvent().setCaller(caller).setSub(sub).setChannel(channel).setChange(change).setMetadata(metadata);
    }

    @Override
    public Event abbreviate() {

        return this;
    }

    @Override
    @JsonIgnore
    public Map<String, String> eventMetadata() {

        return ImmutableMap.of(
                ObjectSchema.SCHEMA, change.getSchema().toString()
        );
    }
}
