package io.basestar.schema;

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
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

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
public class Id {

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

        public Id build() {

            return new Id(this);
        }
    }

    public static Property.Builder builder() {

        return new Property.Builder();
    }

    public Id(final Id.Builder builder) {

        this.expression = builder.getExpression();
        this.constraints = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getConstraints()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
    }

    public String evaluate(final String value, final Context context) {

        if(expression != null) {
            return expression.evaluateAs(String.class, context.with(VAR_VALUE, value));
        } else {
            return value;
        }
    }

    public Set<Constraint.Violation> validate(final Path path, final Object after, final Context context) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Path newPath = path.with(Reserved.ID);
        if(after == null) {
            violations.add(new Constraint.Violation(newPath, Constraint.REQUIRED));
        } else if(!constraints.isEmpty()) {
            final Context newContext = context.with(VAR_VALUE, after);
            for(final Map.Entry<String, Constraint> entry : constraints.entrySet()) {
                final String name = entry.getKey();
                final Constraint constraint = entry.getValue();
                if(!constraint.getExpression().evaluatePredicate(newContext)) {
                    violations.add(new Constraint.Violation(newPath, name));
                }
            }
        }
        return violations;
    }
}
