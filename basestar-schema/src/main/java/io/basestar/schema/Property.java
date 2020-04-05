package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.exception.MissingPropertyException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property
 */

@Getter
public class Property implements Member {

    private static final String VAR_VALUE = "value";

    @JsonIgnore
    private final String name;

    private final String description;

    private final Use<?> type;

    private final boolean required;

    private final boolean immutable;

    private final Expression expression;

    private final SortedMap<String, Constraint> constraints;

    private final Visibility visibility;

    @Data
    @Accessors(chain = true)
    public static class Builder implements Described {

        private Use<?> type;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private String description;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private boolean required;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private boolean immutable;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private final Map<String, Constraint.Builder> constraints = new TreeMap<>();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private Visibility visibility;

        public Property build(final Schema.Resolver resolver, final String name) {

            return new Property(this, resolver, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Property(final Builder builder, final Schema.Resolver schemaResolver, final String name) {

        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
        this.name = name;
        this.description = builder.getDescription();
        this.type = builder.getType().resolve(schemaResolver);
        this.required = builder.isRequired();
        this.immutable = builder.isImmutable();
        this.expression = builder.getExpression();
        this.constraints = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getConstraints()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.visibility = builder.getVisibility();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

        return ((Use<Object>)type).expand(value, expander, expand);
    }

    @Override
    @Deprecated
    public Set<Path> requireExpand(final Set<Path> paths) {

        return type.requireExpand(paths);
    }

    @Override
    public Use<?> typeOf(final Path path) {

        return type.typeOf(path);
    }

    public Object create(final Object value) {

        return type.create(value);
    }

    public <T> T cast(final Object o, final Class<T> as) {

        return type.cast(o, as);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public Multimap<Path, Instance> links(final Object value) {

        return ((Use<Object>)type).refs(value);
    }

    @SuppressWarnings("unchecked")
    public void serialize(final Object value, final DataOutput out) throws IOException {

        ((Use<Object>)type).serialize(value, out);
    }

//    public Map<String, Object> openApiProperty() {
//
//        return type.openApiType();
//    }

    public Object evaluate(final Object value, final Context context) {

        if(expression != null) {
//            final Map<String, Object> newContext = new HashMap<>(context);
//            newContext.put(VAR_VALUE, value);
            return type.create(expression.evaluate(context.with(VAR_VALUE, value)));
        } else {
            return value;
        }
    }

    public Set<Constraint.Violation> validate(final Path path, final Object before, final Object after, final Context context) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Path newPath = path.with(name);
        if(after == null && required) {
            violations.add(new Constraint.Violation(newPath, Constraint.REQUIRED));
        } else if(immutable && !Objects.equals(before, after)) {
            violations.add(new Constraint.Violation(newPath, Constraint.IMMUTABLE));
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

    interface Resolver {

        Map<String, Property> getDeclaredProperties();

        Map<String, Property> getAllProperties();

        default Property getProperty(final String name, final boolean inherited) {

            if(inherited) {
                return getAllProperties().get(name);
            } else {
                return getDeclaredProperties().get(name);
            }
        }

        default Property requireProperty(final String name, final boolean inherited) {

            final Property result = getProperty(name, inherited);
            if (result == null) {
                throw new MissingPropertyException(name);
            } else {
                return result;
            }
        }
    }
}
