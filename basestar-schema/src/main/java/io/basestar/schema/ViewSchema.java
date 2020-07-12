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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseObject;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class ViewSchema implements InstanceSchema, Permission.Resolver {

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    @Nonnull
    private final InstanceSchema from;

    @Nonnull
    private final List<Sort> sort;

    /** Description of the schema */

    @Nullable
    private final String description;

    @Nonnull
    private final SortedMap<String, Property> select;

    @Nonnull
    private final SortedMap<String, Property> group;

    @Nullable
    private final Expression where;

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Object> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends InstanceSchema.Descriptor {

        String TYPE = "view";

        default String type() {

            return TYPE;
        }

        Name getFrom();

        List<Sort> getSort();

        Map<String, Property.Descriptor> getSelect();

        Map<String, Property.Descriptor> getGroup();

        Expression getWhere();

        Map<String, Permission.Descriptor> getPermissions();

        @Override
        default Map<String, Property.Descriptor> getProperties() {

            return ImmutableMap.<String, Property.Descriptor>builder()
                    .putAll(Nullsafe.option(getSelect()))
                    .putAll(Nullsafe.option(getGroup()))
                    .build();
        }

        @Override
        default ViewSchema build(final Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

            return new ViewSchema(this, resolver, qualifiedName, slot);
        }

        @Override
        default ViewSchema build() {

            return new ViewSchema(this, Resolver.Constructing.ANONYMOUS, Schema.anonymousQualifiedName(), Schema.anonymousSlot());
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "from", "select", "group", "permissions", "extensions"})
    public static class Builder implements InstanceSchema.Builder, Descriptor {

        public static final String TYPE = "view";

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private Name from;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> select;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> group;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression where;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Descriptor> permissions;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public String getType() {

            return TYPE;
        }

        public Builder setSelect(final String name, final Property.Builder v) {

            select = Nullsafe.immutableCopyPut(select, name, v);
            return this;
        }

        public Builder setGroup(final String name, final Property.Builder v) {

            group = Nullsafe.immutableCopyPut(group, name, v);
            return this;
        }

        public Builder setPermission(final String name, final Permission.Builder v) {

            permissions = Nullsafe.immutableCopyPut(permissions, name, v);
            return this;
        }

        // FIXME:
        @Override
        public Builder setProperty(final String name, final Property.Descriptor v) {

            throw new UnsupportedOperationException();
        }

        // FIXME:
        @Override
        public Builder setProperties(final Map<String, Property.Descriptor> vs) {

            throw new UnsupportedOperationException();
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private ViewSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.option(descriptor.getVersion(), 1L);
        this.from = resolver.requireInstanceSchema(descriptor.getFrom());
        this.sort = Nullsafe.immutableCopy(descriptor.getSort());
        this.description = descriptor.getDescription();
        this.select = ImmutableSortedMap.copyOf(Nullsafe.option(descriptor.getSelect()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, qualifiedName.with(e.getKey())))));
        this.group = ImmutableSortedMap.copyOf(Nullsafe.option(descriptor.getGroup()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, qualifiedName.with(e.getKey())))));
        this.where = descriptor.getWhere();
        this.declaredProperties = ImmutableSortedMap.<String, Property>orderedBy(Ordering.natural())
                .putAll(select).putAll(group).build();
        this.declaredPermissions = ImmutableSortedMap.copyOf(Nullsafe.option(descriptor.getPermissions()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.extensions = Nullsafe.immutableSortedCopy(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName.toString());
        }
        this.declaredProperties.forEach(ViewSchema::validateProperty);
    }

    private static void validateProperty(final String name, final Property property) {

        if(!property.getConstraints().isEmpty()) {
            throw new SchemaValidationException("View properties cannot have constraints (" + name + ")");
        }
        if(property.getExpression() == null) {
            throw new SchemaValidationException("Every view property must have an expression (" + name + ")");
        }
        property.getType().visit(TypeValidator.INSTANCE);
    }

    @Override
    public Instance create(final Map<String, Object> value, final boolean expand, final boolean suppress) {

        return new Instance(readProperties(value, expand, suppress));
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {

        return Collections.emptySet();
    }

    @Override
    public Map<String, Property> getProperties() {

        return declaredProperties;
    }

    @Override
    public Map<String, Permission> getPermissions() {

        return declaredPermissions;
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

    @Override
    public InstanceSchema getExtend() {

        return null;
    }

    @Override
    public boolean isConcrete() {

        return true;
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return getDeclaredProperties();
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        return getProperties();
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        return getProperty(name, inherited);
    }

    private static class TypeValidator implements Use.Visitor.Defaulting<Void> {

        private static final TypeValidator INSTANCE = new TypeValidator();

        @Override
        public Void visitDefault(final Use<?> type) {

            return null;
        }

        @Override
        public Void visitRef(final UseObject type) {

            throw new SchemaValidationException("View properties cannot use references");
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

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            from.collectDependencies(expand, out);
            select.forEach((k, v) -> v.collectDependencies(expand, out));
            group.forEach((k, v) -> v.collectDependencies(expand, out));
        }
    }

    @Override
    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public Name getFrom() {

                return from.getQualifiedName();
            }

            @Override
            public List<Sort> getSort() {

                return sort;
            }

            @Override
            public Map<String, Property.Descriptor> getSelect() {

                return select.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Map<String, Property.Descriptor> getGroup() {

                return group.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Expression getWhere() {

                return where;
            }

            @Override
            public Map<String, Permission.Descriptor> getPermissions() {

                return declaredPermissions.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Long getVersion() {

                return version;
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
