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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.*;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class ViewSchema implements LinkableSchema {

    public static String KEY = Reserved.PREFIX + "key";

    public static final SortedMap<String, Use<?>> METADATA_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(KEY, UseBinary.DEFAULT)
            .build();

    @Getter
    @RequiredArgsConstructor
    public static class From implements Serializable {

        @Nonnull
        private final LinkableSchema schema;

        @Nonnull
        private final Set<Name> expand;

        public Descriptor.From descriptor() {

            return new Descriptor.From() {
                @Override
                public Name getSchema() {

                    return schema.getQualifiedName();
                }

                @Override
                public Set<Name> getExpand() {

                    return expand;
                }
            };
        }

        @Data
        @Accessors(chain = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonPropertyOrder({"schema", "expand"})
        public static class Builder implements Descriptor.From {

            @Nullable
            private Name schema;

            @Nullable
            @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
            @JsonSerialize(contentUsing = ToStringSerializer.class)
            @JsonDeserialize(using = AbbrevSetDeserializer.class)
            private Set<Name> expand;

            @JsonCreator
            @SuppressWarnings("unused")
            public static Builder fromSchema(final String schema) {

                return fromSchema(Name.parse(schema.toString()));
            }

            public static Builder fromSchema(final Name schema) {

                return new Builder().setSchema(schema);
            }
        }

        public static Builder builder() {

            return new Builder();
        }
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends LinkableSchema.Descriptor<ViewSchema> {

        String TYPE = "view";

        @JsonDeserialize(as = ViewSchema.From.Builder.class)
        interface From {

            Name getSchema();

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Set<Name> getExpand();
        }

        @Override
        default String getType() {

            return TYPE;
        }

        Boolean getMaterialized();

        From getFrom();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Sort> getSort();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<String> getGroup();

        Expression getWhere();

        interface Self extends LinkableSchema.Descriptor.Self<ViewSchema>, Descriptor {

            @Override
            default Boolean getMaterialized() {

                return self().isMaterialized();
            }

            @Override
            default Descriptor.From getFrom() {

                return self().getFrom().descriptor();
            }

            @Override
            default List<Sort> getSort() {

                return self().getSort();
            }

            @Override
            default List<String> getGroup() {

                return self().getGroup();
            }

            @Override
            default Expression getWhere() {

                return self().getWhere();
            }
        }

        @Override
        default ViewSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new ViewSchema(this, resolver, version, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "materialized", "from", "select", "group", "permissions", "extensions"})
    public static class Builder implements LinkableSchema.Builder<Builder, ViewSchema>, Descriptor {

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private Boolean materialized;

        @Nullable
        private From from;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Sort> sort;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<String> group;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Link.Descriptor> links;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> expand;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeserializer.class)
        private Expression where;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Descriptor> permissions;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;

        public ViewSchema.Builder addGroup(final String name) {

            group = Immutable.copyAdd(group, name);
            return this;
        }
    }

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    @Nonnull
    private final From from;

    private final boolean materialized;

    @Nonnull
    private final List<Sort> sort;

    /** Description of the schema */

    @Nullable
    private final String description;

    @Nonnull
    private final List<String> group;

    @Nullable
    private final Expression where;

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Link> declaredLinks;

    @Nonnull
    private final SortedSet<Name> declaredExpand;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    public static Builder builder() {

        return new Builder();
    }

    private ViewSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.materialized = Nullsafe.orDefault(descriptor.getMaterialized());
        final Descriptor.From from = Nullsafe.require(descriptor.getFrom());
        if(from.getSchema() == null) {
            throw new SchemaValidationException(qualifiedName, "View must specify from.schema");
        }
        this.from = new From(resolver.requireLinkableSchema(from.getSchema()), Nullsafe.orDefault(from.getExpand()));
        this.sort = Immutable.copy(descriptor.getSort());
        this.description = descriptor.getDescription();
        this.group = Immutable.copy(descriptor.getGroup());
        this.where = descriptor.getWhere();
        final InferenceContext context = new InferenceContext.FromSchema(this.from.getSchema());
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(),
                (k, v) -> v.build(resolver, context, version, qualifiedName.with(k)));
        this.declaredLinks = Immutable.transformValuesSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredPermissions = Immutable.transformValuesSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.declaredExpand = Immutable.sortedCopy(descriptor.getExpand());
        this.extensions = Immutable.sortedCopy(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName.toString());
        }
        this.declaredProperties.values().forEach(ViewSchema::validateProperty);
    }

    private static void validateProperty(final Property property) {

        if(property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression)");
        }
    }

    @Override
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        final Map<String, Object> result = new HashMap<>(readProperties(context, value, expand));
        result.putAll(readMeta(context, value));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if(expand != null && !expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            getLinks().forEach((name, link) -> {
                if(value.containsKey(name)) {
                    result.put(name, link.create(context, value.get(name), branches.get(name)));
                }
            });
        }
        return new Instance(result);
    }

    public byte[] key(final Map<String, Object> value) {

        final List<Object> key = new ArrayList<>();
        if(group.isEmpty()) {
            key.add(value.get(from.getSchema().id()));
        } else {
            group.forEach(name -> key.add(value.get(name)));
        }
        return UseBinary.binaryKey(key);
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        return new Instance(InstanceSchema.deserializeProperties(in));
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {

        return Collections.emptySet();
    }

    @Override
    public Map<String, Property> getProperties() {

        return declaredProperties;
    }

    public Map<String, Property> getSelectProperties() {

        return declaredProperties.entrySet().stream().filter(e -> !group.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, Property> getGroupProperties() {

        return declaredProperties.entrySet().stream().filter(e -> group.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, Permission> getPermissions() {

        return declaredPermissions;
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return METADATA_SCHEMA;
    }

    @Override
    public UseView typeOf() {

        return new UseView(this);
    }

    @Override
    public String id() {

        return KEY;
    }

    @Override
    public boolean isConcrete() {

        return true;
    }

    @Override
    public Map<String, Link> getLinks() {

        return getDeclaredLinks();
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return ImmutableMap.<String, Member>builder()
                .putAll(getDeclaredProperties())
                .putAll(getDeclaredLinks())
                .build();
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        return ImmutableMap.<String, Member>builder()
                .putAll(getProperties())
                .putAll(getLinks())
                .build();
    }

    public Set<Name> getExpand() {

        return declaredExpand;
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        final Property property = getProperty(name, inherited);
        if(property != null) {
            return property;
        }
        return getLink(name, inherited);
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            from.getSchema().collectDependencies(expand, out);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
        }
    }

    public boolean isGrouping() {

        return !group.isEmpty();
    }

    public boolean isAggregating() {

        return getProperties().values().stream().map(Property::getExpression)
                .filter(Objects::nonNull).anyMatch(Expression::isAggregate);
    }

    private static Property.Descriptor viewPropertyDescriptor(final Property.Descriptor descriptor, final From from) {

        final InstanceSchema fromSchema = from.getSchema();
        return (new Property.Descriptor() {

            @Override
            public Map<String, Serializable> getExtensions() {

                return descriptor.getExtensions();
            }

            @Nullable
            @Override
            public String getDescription() {

                return descriptor.getDescription();
            }

            @Override
            public Visibility getVisibility() {

                return descriptor.getVisibility();
            }

            @Override
            public Use<?> getType() {

                if(descriptor.getType() == null && descriptor.getExpression() != null) {
                    final InferenceContext context = new InferenceContext.FromSchema(fromSchema);
                    final Use<?> type = new InferenceVisitor(context).visit(descriptor.getExpression());
                    if(type instanceof UseAny) {
                        throw new IllegalStateException("Cannot infer type from expression " + descriptor.getExpression());
                    } else {
                        return type;
                    }
                } else {
                    return descriptor.getType();
                }
            }

            @Override
            @Deprecated
            public Boolean getRequired() {

                return descriptor.getRequired();
            }

            @Override
            public Boolean getImmutable() {

                return descriptor.getImmutable();
            }

            @Override
            public Expression getExpression() {

                return descriptor.getExpression();
            }

            @Override
            public Serializable getDefault() {

                return descriptor.getDefault();
            }

            @Override
            public List<? extends Constraint> getConstraints() {

                return descriptor.getConstraints();
            }
        });
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> ViewSchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof ViewSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }
}
