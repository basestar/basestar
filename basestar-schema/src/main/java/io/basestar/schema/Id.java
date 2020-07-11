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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides (limited) customization of automatic id generation for objects, specifically a generating expression, and
 * constraints. There is no default implementation, but the default behaviour (schema.id: null|undefined) is
 * caller-provided ids with no constraints, defaulting to generated UUIDs when the caller does not provide an id.
 *
 * This behaviour may be emulated with the id-expression:
 *
 * <code>value ?? sys.uuid()</code>
 *
 * Id-expressions are only evaluated during create, and a value is calculated before any other property expressions,
 * transients, or constraints are applied. It is therefore suggested that only immutable, required properties are used
 * in id-expressions.
 */

@Getter
public class Id implements Serializable {

    private static final String VAR_VALUE = "value";

    private final Expression expression;

    private final SortedMap<String, Constraint> constraints;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private final Map<String, Constraint.Builder> constraints = new TreeMap<>();

        public Id build(final Name qualifiedName) {

            return new Id(this, qualifiedName);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Id(final Builder builder, final Name qualifiedName) {

        this.expression = builder.getExpression();
        this.constraints = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getConstraints()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(qualifiedName.with(e.getKey())))));
    }

    public String evaluate(final String value, final Context context) {

        if(expression != null) {
            return expression.evaluateAs(String.class, context.with(VAR_VALUE, value));
        } else {
            return value;
        }
    }

    public Set<Constraint.Violation> validate(final Name path, final Object after, final Context context) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Name newName = path.with(Reserved.ID);
        if(after == null) {
            violations.add(new Constraint.Violation(newName, Constraint.REQUIRED));
        } else if(!constraints.isEmpty()) {
            final Context newContext = context.with(VAR_VALUE, after);
            for(final Map.Entry<String, Constraint> entry : constraints.entrySet()) {
                final String name = entry.getKey();
                final Constraint constraint = entry.getValue();
                if(!constraint.getExpression().evaluatePredicate(newContext)) {
                    violations.add(new Constraint.Violation(newName, name));
                }
            }
        }
        return violations;
    }
}
