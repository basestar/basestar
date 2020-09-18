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
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Subscription {

    public static final Comparator<Subscription> COMPARATOR = Comparator.comparing(Subscription::getSub).thenComparing(Subscription::getChannel);

    private String sub;

    private String channel;

    private Caller caller;

    @JsonDeserialize(using = ExpressionDeserializer.class)
    @JsonSerialize(using = ToStringSerializer.class)
    private Expression expression;

    private SubscriptionInfo info;

    @Data
    public static class Id {

        private final String sub;

        private final String channel;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Key {

        @JsonDeserialize(using = NameDeserializer.class)
        @JsonSerialize(using = ToStringSerializer.class)
        private Name schema;

        private String index;

        private List<Object> partition;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Term {

        @JsonDeserialize(using = NameDeserializer.class)
        @JsonSerialize(using = ToStringSerializer.class)
        private Name schema;

        private Expression expression;

        private Set<Name> expand;
    }
}
