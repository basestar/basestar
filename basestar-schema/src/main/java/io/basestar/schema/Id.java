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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.use.UseString;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private final List<Constraint> constraints;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor {

        Expression getExpression();

        List<? extends Constraint> getConstraints();

        default Id build(final Name qualifiedName) {

            return new Id(this, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Constraint> constraints;
    }

    public static Builder builder() {

        return new Builder();
    }

    public Id(final Descriptor descriptor, final Name qualifiedName) {

        this.expression = descriptor.getExpression();
        this.constraints = ImmutableList.copyOf(Nullsafe.orDefault(descriptor.getConstraints()));
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
        final Name qualifiedName = path.with(ObjectSchema.ID);
        if(after == null) {
            violations.add(new Constraint.Violation(qualifiedName, Constraint.REQUIRED, null, ImmutableSet.of()));
        } else if(!constraints.isEmpty()) {
            final Context newContext = context.with(VAR_VALUE, after);
            for (final Constraint constraint : constraints) {
                violations.addAll(constraint.violations(UseString.DEFAULT, newContext, qualifiedName, after));
            }
        }
        return violations;
    }

    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public Expression getExpression() {

                return expression;
            }

            @Override
            public List<? extends Constraint> getConstraints() {

                return constraints;
            }
        };
    }
}
