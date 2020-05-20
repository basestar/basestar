package io.basestar.schema.aggregate;

/*-
 * #%L
 * basestar-schema
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
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import lombok.Builder;
import lombok.Data;

@Data
@JsonDeserialize(builder = Count.Builder.class)
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class Count implements Aggregate {

    public static final String TYPE = "count";

    private final Expression output;

    @Override
    public <T> T visit(final AggregateVisitor<T> visitor) {

        return visitor.visitCount(this);
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression output;
    }
}
