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
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseRef;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Member.Builder {

        private Use<?> type;

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        @Nullable
        @JsonSetter(contentNulls = Nulls.FAIL)
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        @JsonDeserialize(contentUsing = PathDeserializer.class)
        private Set<Name> expand;

        @Nullable
        private Visibility visibility;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public Transient build(final Name qualifiedName) {

            return new Transient(this, qualifiedName);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Transient(final Builder builder, final Name qualifiedName) {

        this.qualifiedName = qualifiedName;
        this.type = builder.getType();
        this.description = builder.getDescription();
        this.expression =  Nullsafe.require(builder.getExpression());
        this.visibility = builder.getVisibility();
        this.expand = Nullsafe.immutableSortedCopy(builder.getExpand());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        if(type != null) {
            type.visit(TypeValidator.INSTANCE);
        }
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
    }

    public boolean isTyped() {

        return type != null;
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

    //FIXME
    @Override
    @SuppressWarnings("unchecked")
    public <T> Use<T> typeOf(final Name name) {

        if(type != null) {
            return (Use<T>)type.typeOf(name);
        } else {
            throw new UnsupportedOperationException();
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

    private static class TypeValidator implements Use.Visitor.Defaulting<Void> {

        private static final TypeValidator INSTANCE = new TypeValidator();

        @Override
        public Void visitDefault(final Use<?> type) {

            return null;
        }

        @Override
        public Void visitRef(final UseRef type) {

            throw new SchemaValidationException("Transients cannot use references");
        }

        @Override
        public <T> Void visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

            return type.getType().visit(this);
        }

        @Override
        public <T> Void visitMap(final UseMap<T> type) {

            return type.getType().visit(this);
        }
    }
}
