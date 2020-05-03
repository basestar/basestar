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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property
 */

@Getter
public class Property implements Member {

    @Nonnull
    @JsonIgnore
    private final String name;

    @Nullable
    private final String description;

    @Nonnull
    private final Use<?> type;

    private final boolean required;

    private final boolean immutable;

    @Nullable
    private final Expression expression;

    @Nonnull
    private final SortedMap<String, Constraint> constraints;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final Map<String, Object> extensions;

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

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

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
        this.constraints = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getConstraints()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.visibility = builder.getVisibility();
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {

        return ((Use<Object>)type).expand(value, expander, expand);
    }

    @Override
    @Deprecated
    public Set<Path> requiredExpand(final Set<Path> paths) {

        return type.requiredExpand(paths);
    }

    @Override
    public Use<?> typeOf(final Path path) {

        return type.typeOf(path);
    }

    @Override
    public Set<Path> transientExpand(final Path path, final Set<Path> expand) {

        return type.transientExpand(path, expand);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object applyVisibility(final Context context, final Object value) {

        return ((Use<Object>)type).applyVisibility(context, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluateTransients(final Context context, final Object value, final Set<Path> expand) {

        return ((Use<Object>)type).evaluateTransients(context, value, expand);
    }

    public Object create(final Object value, final boolean expand, final boolean suppress) {

        return type.create(value, expand, suppress);
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

    public Object evaluate(final Context context, final Object value) {

        if(expression != null) {
//            final Map<String, Object> newContext = new HashMap<>(context);
//            newContext.put(VAR_VALUE, value);
            return type.create(expression.evaluate(context.with(VAR_VALUE, value)), true, false);
        } else {
            return value;
        }
    }

    public Set<Constraint.Violation> validate(final Context context, final Path path, final Object after) {

        return validate(context, path, after, after);
    }

    // FIXME: immutability check should be implemented differently

    @SuppressWarnings("unchecked")
    public Set<Constraint.Violation> validate(final Context context, final Path path, final Object before, final Object after) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Path newPath = path.with(name);
        if(after == null && required) {
            violations.add(new Constraint.Violation(newPath, Constraint.REQUIRED));
        } else if(immutable && !Objects.equals(before, after)) {
            violations.add(new Constraint.Violation(newPath, Constraint.IMMUTABLE));
        } else {
            violations.addAll(((Use<Object>)type).validate(context, newPath, after));
            if (!constraints.isEmpty()) {
                final Context newContext = context.with(VAR_VALUE, after);
                for (final Map.Entry<String, Constraint> entry : constraints.entrySet()) {
                    final String name = entry.getKey();
                    final Constraint constraint = entry.getValue();
                    if (!constraint.getExpression().evaluatePredicate(newContext)) {
                        violations.add(new Constraint.Violation(newPath, name));
                    }
                }
            }
        }
        return violations;
    }

    public interface Resolver {

        Map<String, Property> getDeclaredProperties();

        Map<String, Property> getProperties();

        default Property getProperty(final String name, final boolean inherited) {

            if(inherited) {
                return getProperties().get(name);
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
