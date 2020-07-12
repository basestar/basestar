package io.basestar.schema;

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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

@Getter
public class Constraint implements Named, Described, Serializable {

    public static final String REQUIRED = "required";

    public static final String IMMUTABLE = "immutable";

    @Nonnull
    private final Name qualifiedName;

    @Nullable
    private final String description;

    @Nonnull
    private final Expression expression;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Described {

        Expression getExpression();

        default Constraint build(final Name qualifiedName) {

            return new Constraint(this, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;
    }

    public static Builder builder() {

        return new Builder();
    }

    private Constraint(final Descriptor descriptor, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.description = descriptor.getDescription();
        this.expression = Nullsafe.require(descriptor.getExpression());
    }

    @Data
    public static class Violation {

        @JsonSerialize(using = ToStringSerializer.class)
        private final Name name;

        private final String constraint;
    }

    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public Expression getExpression() {

                return expression;
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }
        };
    }
}
