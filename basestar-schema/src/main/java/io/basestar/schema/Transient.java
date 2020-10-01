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
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.*;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Transient
 */

@Getter
@Accessors(chain = true)
public class Transient implements Member {

    @Nonnull
    private final Name qualifiedName;

    @Nullable
    private final Use<?> type;

    @Nullable
    private final String description;

    @Nonnull
    private final Expression expression;

    @Nullable
    private final Visibility visibility;

    // FIXME: what does this do?

    @Nonnull
    private final SortedSet<Name> expand;

    @Nonnull
    private final Map<String, Object> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Member.Descriptor {

        Use<?> getType();

        Expression getExpression();

        Set<Name> getExpand();

        default Transient build(final Name qualifiedName) {

            return new Transient(this, qualifiedName);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        private Use<?> type;

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression expression;

        @Nullable
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> expand;

        @Nullable
        private Visibility visibility;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private Transient(final Descriptor descriptor, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.type = descriptor.getType();
        this.description = descriptor.getDescription();
        this.expression =  Nullsafe.require(descriptor.getExpression());
        this.visibility = descriptor.getVisibility();
        this.expand = Nullsafe.immutableSortedCopy(descriptor.getExpand());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        if(type != null) {
            type.visit(new TypeValidator(qualifiedName));
        }
        this.extensions = Nullsafe.immutableSortedCopy(descriptor.getExtensions());
    }

    public boolean isTyped() {

        return type != null;
    }

    @Override
    public boolean supportsTrivialJoin(final Set<Name> expand) {

        return false;
    }

    @Override
    public Optional<Use<?>> layout(final Set<Name> expand) {

        return Optional.empty();
    }

    @Override
    public Object expand(final Object value, final Expander expander, final Set<Name> expand) {

        return value;
    }

    @Override
    public Object applyVisibility(final Context context, final Object value) {

        return value;
    }

    @Override
    public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {

        final Object raw = expression.evaluate(context);
        if(type != null) {
            return type.create(raw);
        } else {
            return raw;
        }
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return Collections.emptySet();
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        // FIXME
        return Collections.emptySet();
    }

    @Override
    public Object create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(type == null) {
            return value;
        } else {
            return type.create(value, expand, suppress);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Use<T> typeOf(final Name name) {

        if(type != null) {
            return (Use<T>)type.typeOf(name);
        } else {
            return (Use<T>)UseAny.DEFAULT;
        }
    }

    @Override
    public Type javaType(final Name name) {

        if(type != null) {
            return type.javaType(name);
        } else {
            return Object.class;
        }
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        final Set<Name> result = new HashSet<>();
        this.expand.forEach(p -> {
            if(p.isChild(Name.of(Schema.VAR_THIS))) {
                // Move expand from this to expand on the parameter path, have to remove the
                // last parent path element, because it will point to this transient
                result.add(name.withoutLast().with(p.withoutFirst()));
            } else {
                result.add(p);
            }
        });
        return result;
    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        return ImmutableSet.of(Name.of());
    }

    public interface Resolver {

        interface Builder {

            Builder setTransient(String name, Transient.Descriptor v);

            Builder setTransients(Map<String, Transient.Descriptor> vs);
        }

        Map<String, Transient> getDeclaredTransients();

        Map<String, Transient> getTransients();

        default Transient getTransient(final String name, final boolean inherited) {

            if(inherited) {
                return getTransients().get(name);
            } else {
                return getDeclaredTransients().get(name);
            }
        }

        default Transient requireTransient(final String name, final boolean inherited) {

            final Transient result = getTransient(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }

    @RequiredArgsConstructor
    private static class TypeValidator implements Use.Visitor.Defaulting<Void> {

        private final Name qualifiedName;

        @Override
        public <T> Void visitDefault(final Use<T> type) {

            return null;
        }

        @Override
        public Void visitObject(final UseObject type) {

            throw new SchemaValidationException(qualifiedName,  "Transients cannot use references");
        }

        @Override
        public <V, T extends Collection<V>> Void visitCollection(final UseCollection<V, T> type) {

            return type.getType().visit(this);
        }

        @Override
        public <T> Void visitMap(final UseMap<T> type) {

            return type.getType().visit(this);
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
            public Expression getExpression() {

                return expression;
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Visibility getVisibility() {

                return visibility;
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }

            @Override
            public Map<String, Object> getExtensions() {

                return extensions;
            }
        };
    }
}
