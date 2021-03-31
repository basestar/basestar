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
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.schema.exception.MissingPropertyException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseScalar;
import io.basestar.schema.util.*;
import io.basestar.util.Immutable;
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
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final boolean immutable;

    @Nullable
    private final Serializable defaultValue;

    @Nullable
    private final Expression expression;

    @Nonnull
    private final List<Constraint> constraints;

    @Nullable
    private final Visibility visibility;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Member.Descriptor {

        Use<?> getType();

        @Deprecated
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getRequired();

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        Boolean getImmutable();

        Expression getExpression();

        Serializable getDefault();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<? extends Constraint> getConstraints();

        default Property build(final Schema.Resolver resolver, final Version version, final Name qualifiedName) {

            return build(resolver, null, version, qualifiedName);
        }

        default Property build(final Schema.Resolver resolver, final InferenceContext context, final Version version, final Name qualifiedName) {

            return new Property(this, resolver, context, version, qualifiedName);
        }

        interface Self extends Descriptor, Member.Descriptor.Self<Property> {

            @Override
            default Use<?> getType() {

                return self().typeOf();
            }

            @Override
            default Boolean getRequired() {

                return false;
            }

            @Override
            default Boolean getImmutable() {

                return self().isImmutable();
            }

            @Override
            default Expression getExpression() {

                return self().getExpression();
            }

            @Override
            default Serializable getDefault() {

                return self().getDefaultValue();
            }

            @Override
            default List<? extends Constraint> getConstraints() {

                return self().getConstraints();
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonPropertyOrder({"type", "description", "immutable", "expression", "constraints", "visibility", "extensions"})
    public static class Builder implements Descriptor, Member.Builder<Builder> {

        private Use<?> type;

        private String description;

        @Deprecated
        private Boolean required;

        private Boolean immutable;

        @Getter(AccessLevel.NONE)
        @Setter(AccessLevel.NONE)
        private Serializable defaultValue;

        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<? extends Constraint> constraints;

        private Visibility visibility;

        @Nullable
        private Map<String, Serializable> extensions;

        public Serializable getDefault() {

            return defaultValue;
        }

        public void setDefault(final Serializable value) {

            this.defaultValue = value;
        }

        @JsonCreator
        @SuppressWarnings("unused")
        public static Property.Builder fromExpression(final String expression) {

            return new Property.Builder()
                    .setExpression(Expression.parse(expression));
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Property(final Descriptor builder, final Schema.Resolver schemaResolver, final InferenceContext context, final Version version, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.description = builder.getDescription();
        this.type = legacyFix(qualifiedName, Member.type(builder.getType(), builder.getExpression(), context).resolve(schemaResolver), builder.getRequired(), version);
        this.defaultValue = (Serializable)Nullsafe.map(builder.getDefault(), type::create);
        this.immutable = Nullsafe.orDefault(builder.getImmutable());
        this.expression = builder.getExpression();
        this.constraints = Immutable.list(builder.getConstraints());
        this.visibility = builder.getVisibility();
        this.extensions = Immutable.sortedMap(builder.getExtensions());
    }

    private static Use<?> legacyFix(final Name qualifiedName, final Use<?> type, final Boolean required, final Version version) {

        if(version == Version.LEGACY) {
            return type.optional(!Nullsafe.orDefault(required));
        } else if(required != null) {
            throw new SchemaValidationException(qualifiedName, "Required is now deprecated, use 'type?' instead");
        } else {
            return type;
        }
    }

    @Override
    public Use<?> typeOf() {

        return type;
    }

    @Override
    public boolean supportsTrivialJoin(final Set<Name> expand) {

        return type.visit(new Use.Visitor.Defaulting<Boolean>() {

            @Override
            public <T> Boolean visitDefault(final Use<T> type) {

                return false;
            }

            @Override
            public <T> Boolean visitScalar(final UseScalar<T> type) {

                return true;
            }

            @Override
            public Boolean visitInstance(final UseInstance type) {

                return type.getSchema().supportsTrivialJoin(expand);
            }
        });
    }

    @Override
    public boolean requiresMigration(final Member member, final Widening widening) {

        if(!(member instanceof Property)) {
            return expression == null;
        }
        final Property target = (Property)member;
        if(!Objects.equals(expression, target.getExpression())) {
            return true;
        }
        return !widening.canWiden(type, target.typeOf());
    }

    public TypedExpression<?> getTypedExpression() {

        return expression == null ? null : TypedExpression.from(expression, type);
    }

    @Override
    public Optional<Use<?>> layout(final Set<Name> expand) {

        return Optional.of(typeOf());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object expand(final Name parent, final Object value, final Expander expander, final Set<Name> expand) {

        return ((Use<Object>)type).expand(parent, value, expander, expand);
    }

    @Override
    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

        if(expand != null) {
            type.expand(parent, expander, expand);
        }
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
    public Type javaType(final Name name) {

        return type.javaType(name);
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
    public Object create(final ValueContext context, final Object value, final Set<Name> expand) {

        try {
            return type.create(context, value, expand);
        } catch (final UnexpectedTypeException e) {
            throw new ConstraintViolationException(ImmutableSet.of(
                    new Constraint.Violation(getQualifiedName(), "type", e.getMessage(), ImmutableSet.of())
            ));
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T cast(final Object o, final Class<T> as) {

        return ((Use<T>)type).cast(o, as);
    }

    @SuppressWarnings("unchecked")
    public void serialize(final Object value, final DataOutput out) throws IOException {

        ((Use<Object>)type).serialize(value, out);
    }

    public Object evaluate(final Context context, final Set<Name> expand, final Object value) {

        if(expression != null) {
            return type.create(expression.evaluate(context.with(VAR_VALUE, value)), expand, false);
        } else {
            return value;
        }
    }

    public Set<Constraint.Violation> validate(final Context context, final Name name, final Object after) {

        return validate(context, name, after, after);
    }

    @SuppressWarnings("unchecked")
    public Set<Constraint.Violation> validate(final Context context, final Name path, final Object before, final Object after) {

        final Set<Constraint.Violation> violations = new HashSet<>();
        final Name qualifiedName = path.with(getName());
        if(immutable && !Objects.equals(before, after)) {
            violations.add(new Constraint.Violation(qualifiedName, Constraint.IMMUTABLE, null, ImmutableSet.of()));
        } else {
            violations.addAll(((Use<Object>)type).validate(context, qualifiedName, after));
            if (!constraints.isEmpty()) {
                final Context newContext = context.with(VAR_VALUE, after);
                for (final Constraint constraint : constraints) {
                    violations.addAll(constraint.violations(type, newContext, qualifiedName, after));
                }
            }
        }
        return violations;
    }

    public Property extend(final Property ext) {

        return ext;
    }

    public static SortedMap<String, Property> extend(final Map<String, Property> base, final Map<String, Property> ext) {

        return Immutable.sortedMerge(base, ext, Property::extend);
    }

    public static SortedMap<String, Property> extend(final Collection<? extends Resolver> base, final Map<String, Property> ext) {

        return Immutable.sortedMap(Stream.concat(
                base.stream().map(Resolver::getProperties),
                Stream.of(ext)
        ).reduce(Property::extend).orElse(Collections.emptyMap()));
    }

    public interface Resolver {

        interface Descriptor {

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, Property.Descriptor> getProperties();
        }

        interface Builder<B extends Builder<B>> extends Descriptor {

            default B setProperty(final String name, final Property.Descriptor v) {

                return setProperties(Immutable.put(getProperties(), name, v));
            }

            B setProperties(Map<String, Property.Descriptor> vs);
        }

        Map<String, Property> getDeclaredProperties();

        Map<String, Property> getProperties();

        default Map<String, Property.Descriptor> describeDeclaredProperties() {

            return getDeclaredProperties().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().descriptor()
            ));
        }

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

        return (Descriptor.Self) () -> Property.this;
    }
}
