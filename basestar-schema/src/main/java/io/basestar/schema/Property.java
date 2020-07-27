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

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.exception.MissingPropertyException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Property
 */

@Getter
public class Property implements Member {

    @Nonnull
    @JsonIgnore
    private final Name qualifiedName;

    @Nullable
    private final String description;

    @Nonnull
    private final Use<?> type;

    // Replaced with nullable on Use<T>
    @Deprecated
    private final boolean required;

    private final boolean immutable;

    @Nullable
    private final Object defaultValue;

    @Nullable
    private final Expression expression;

    @Nonnull
    private final List<Constraint> constraints;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final Map<String, Object> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Member.Descriptor {

        Use<?> getType();

        @Deprecated
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getRequired();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getImmutable();

        Expression getExpression();

        Object getDefault();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<? extends Constraint> getConstraints();

        default Property build(final Schema.Resolver resolver, final Name qualifiedName) {

            return new Property(this, resolver, qualifiedName);
        }

        interface Delegating extends Descriptor, Member.Descriptor.Delegating {

            @Override
            Descriptor delegate();

            @Override
            default Use<?> getType() {

                return delegate().getType();
            }

            @Override
            @Deprecated
            default Boolean getRequired() {

                return delegate().getRequired();
            }

            @Override
            default Boolean getImmutable() {

                return delegate().getImmutable();
            }

            @Override
            default Expression getExpression() {

                return delegate().getExpression();
            }

            @Override
            default Object getDefault() {

                return delegate().getDefault();
            }

            @Override
            default List<? extends Constraint> getConstraints() {

                return delegate().getConstraints();
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonPropertyOrder({"type", "description", "required", "immutable", "expression", "constraints", "visibility", "extensions"})
    public static class Builder implements Descriptor, Member.Builder {

        private Use<?> type;

        private String description;

        @Deprecated
        private Boolean required;

        private Boolean immutable;

        @Getter(AccessLevel.NONE)
        @Setter(AccessLevel.NONE)
        private Object defaultValue;

        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<? extends Constraint> constraints;

        private Visibility visibility;

        @Nullable
        private Map<String, Object> extensions;

        public Object getDefault() {

            return defaultValue;
        }

        public void setDefault(final Object value) {

            this.defaultValue = value;
        }

        @JsonCreator
        @SuppressWarnings("unused")
        public static Builder fromExpression(final String expression) {

            return new Builder()
                    .setExpression(Expression.parse(expression));
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Property(final Descriptor builder, final Schema.Resolver schemaResolver, final Name qualifiedName) {

        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        this.qualifiedName = qualifiedName;
        this.description = builder.getDescription();
        this.type = builder.getType().resolve(schemaResolver);
        this.required = Nullsafe.option(builder.getRequired());
        this.defaultValue = type.create(builder.getDefault());
        this.immutable = Nullsafe.option(builder.getImmutable());
        this.expression = builder.getExpression();
        this.constraints = Nullsafe.immutableCopy(builder.getConstraints());
        this.visibility = builder.getVisibility();
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
    }

    @Override
    public Optional<Use<?>> layout(final Set<Name> expand) {

        return Optional.of(getType().nullable(!required));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object expand(final Object value, final Expander expander, final Set<Name> expand) {

        return ((Use<Object>)type).expand(value, expander, expand);
    }

    @Override
    @Deprecated
    public Set<Name> requiredExpand(final Set<Name> names) {

        return type.requiredExpand(names);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Use<T> typeOf(final Name name) {

        return (Use<T>)type.typeOf(name);
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return type.transientExpand(name, expand);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object applyVisibility(final Context context, final Object value) {

        return ((Use<Object>)type).applyVisibility(context, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {

        return ((Use<Object>)type).evaluateTransients(context, value, expand);
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return type.refQueries(otherSchemaName, expand, name);
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return type.refExpand(otherSchemaName, expand);
    }

    @SuppressWarnings("unchecked")
    public Map<Ref, Long> refVersions(final Object value) {

        return ((Use<Object>)type).refVersions(value);
    }

    @Override
    public Object create(final Object value, final boolean expand, final boolean suppress) {

        return type.create(value, expand, suppress);
    }

    public <T> T cast(final Object o, final Class<T> as) {

        return type.cast(o, as);
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

    public Set<Constraint.Violation> validate(final Context context, final Name name, final Object after) {

        return validate(context, name, after, after);
    }
/*

 */
    // FIXME: immutability check should be implemented differently

    @SuppressWarnings("unchecked")
    public Set<Constraint.Violation> validate(final Context context, final Name path, final Object before, final Object after) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Name qualifiedName = path.with(getName());
        if(after == null && required) {
            violations.add(new Constraint.Violation(qualifiedName, Constraint.REQUIRED, null));
        } else if(immutable && !Objects.equals(before, after)) {
            violations.add(new Constraint.Violation(qualifiedName, Constraint.IMMUTABLE, null));
        } else {
//            violations.addAll(((Use<Object>)type).validate(context, qualifiedName, after));
//            if (!constraints.isEmpty()) {
//                final Context newContext = context.with(VAR_VALUE, after);
//                for (final Constraint constraint : constraints) {
//                    violations.addAll(constraint.violations(type, newContext, qualifiedName, after));
//                }
//            }
        }
        return violations;
    }

    public interface Resolver {

        interface Builder {

            Builder setProperty(String name, Property.Descriptor v);

            Builder setProperties(Map<String, Property.Descriptor> vs);
        }

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

    @Override
    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public Use<?> getType() {

                return type;
            }

            @Override
            public String getDescription() {

                return description;
            }

            @Override
            @Deprecated
            public Boolean getRequired() {

                return required;
            }

            @Override
            public Boolean getImmutable() {

                return immutable;
            }

            @Override
            public Expression getExpression() {

                return expression;
            }

            @Override
            public Object getDefault() {

                return defaultValue;
            }

            @Override
            public List<? extends Constraint> getConstraints() {

                return constraints;
            }

            @Override
            public Visibility getVisibility() {

                return visibility;
            }

            @Override
            public Map<String, Object> getExtensions() {

                return extensions;
            }
        };
    }
}
