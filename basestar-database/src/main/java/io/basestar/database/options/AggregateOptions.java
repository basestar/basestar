package io.basestar.database.options;

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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.database.serde.AggregateGroupDeserializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.aggregate.Aggregate;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@JsonDeserialize(builder = AggregateOptions.Builder.class)
@Builder(toBuilder = true, builderClassName = "Builder", setterPrefix = "set")
public class AggregateOptions {

    public static final String TYPE = "aggregate";

    private final String schema;

    private final Expression filter;

    private final Map<String, Expression> group;

    private final Map<String, Aggregate> aggregate;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        private String schema;

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression filter;

        @JsonDeserialize(using = AggregateGroupDeserializer.class)
        private Map<String, Expression> group;

        private Map<String, Aggregate> aggregate;
    }
}
