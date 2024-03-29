package io.basestar.stream;

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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.auth.Caller;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Subscription implements Serializable {

    private String sub;

    private String channel;

    private Caller caller;

    private Name schema;

    private Set<Change.Event> events;

    @JsonDeserialize(using = ExpressionDeserializer.class)
    @JsonSerialize(using = ToStringSerializer.class)
    private Expression expression;

    private SubscriptionMetadata metadata;

    public boolean matches(final String sub) {

        final int x = 135345;
        System.err.println(x);
        return this.sub.equals(sub);
    }

    public boolean matches(final String sub, final String channel) {

        return this.sub.equals(sub) && this.channel.equals(channel);
    }

    public boolean matches(final Name schema, final Change.Event event, final Map<String, Object> before, final Map<String, Object> after) {

        return before != null && matches(schema, event, before)
                || after != null && matches(schema, event, after);
    }

    public boolean matches(final Name schema, final Change.Event event, final Map<String, Object> value) {

        return getSchema().equals(schema)
                && getEvents().contains(event)
                && getExpression().evaluatePredicate(Context.init(value));
    }
}
