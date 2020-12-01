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
import io.basestar.expression.constant.NameConstant;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.AbbrevSetDeserializer;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseView;
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
public class ViewSchema implements LinkableSchema, Permission.Resolver, Link.Resolver {

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
    public interface Descriptor extends LinkableSchema.Descriptor {

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

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Link.Descriptor> getLinks();

        Expression getWhere();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Permission.Descriptor> getPermissions();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        @Override
        default ViewSchema build(final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new ViewSchema(this, resolver, version, qualifiedName, slot);
        }

        @Override
        default ViewSchema build(final Name qualifiedName) {

            return build(Resolver.Constructing.ANONYMOUS, Version.CURRENT, qualifiedName, Schema.anonymousSlot());
        }

        @Override
        default ViewSchema build() {

            return build(Schema.anonymousQualifiedName());
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "materialized", "from", "select", "group", "permissions", "extensions"})
    public static class Builder implements InstanceSchema.Builder, Descriptor, Link.Resolver.Builder {

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

        public Builder addGroup(final String name) {

            group = Nullsafe.immutableCopyAdd(group, name);
            return this;
        }

        public Builder setPermission(final String name, final Permission.Builder v) {

            permissions = Nullsafe.immutableCopyPut(permissions, name, v);
            return this;
        }

        @Override
        public Link.Resolver.Builder setLink(final String name, final Link.Descriptor v) {

            links = Nullsafe.immutableCopyPut(links, name, v);
            return this;
        }

        @Override
        public Builder setProperty(final String name, final Property.Descriptor v) {

            properties = Nullsafe.immutableCopyPut(properties, name, v);
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
        this.sort = Nullsafe.immutableCopy(descriptor.getSort());
        this.description = descriptor.getDescription();
        this.group = Nullsafe.immutableCopy(descriptor.getGroup());
        this.where = descriptor.getWhere();
        this.declaredProperties = Nullsafe.immutableSortedCopy(descriptor.getProperties(),
                (k, v) -> viewPropertyDescriptor(v, this.from).build(resolver, version, qualifiedName.with(k)));
        this.declaredLinks = Nullsafe.immutableSortedCopy(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredPermissions = Nullsafe.immutableSortedCopy(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.declaredExpand = Nullsafe.immutableSortedCopy(descriptor.getExpand());
        this.extensions = Nullsafe.immutableSortedCopy(descriptor.getExtensions());
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
    public Instance create(final Map<String, Object> value, final Set<Name> expand, final boolean suppress) {

        final Map<String, Object> result = new HashMap<>(readProperties(value, expand, suppress));
        result.putAll(readMeta(value, suppress));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if(expand != null && !expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            getLinks().forEach((name, link) -> {
                if(value.containsKey(name)) {
                    result.put(name, link.create(value.get(name), branches.get(name), suppress));
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
    public UseView use() {

        return new UseView(this);
    }

    @Override
    public String id() {

        return KEY;
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

        return getProperty(name, inherited);
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            from.getSchema().collectDependencies(expand, out);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
        }
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
                    if (descriptor.getExpression() instanceof NameConstant) {
                        final Name name = ((NameConstant) descriptor.getExpression()).getName();
                        if(name.size() == 1) {
                            final Use<?> metadataType = fromSchema.metadataSchema().get(name.last());
                            if(metadataType != null) {
                                return metadataType;
                            }
                            final Property prop = fromSchema.getProperty(name.last(), true);
                            if(prop != null) {
                                return prop.getType();
                            }
                        }
                    }
                    throw new IllegalStateException("Cannot infer type from expression " + descriptor.getExpression());
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
            public Object getDefault() {

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

        return new Descriptor() {

            @Override
            public Boolean getMaterialized() {

                return materialized;
            }

            @Override
            public From getFrom() {

                return from.descriptor();
            }

            @Override
            public List<Sort> getSort() {

                return sort;
            }

            @Override
            public List<String> getGroup() {

                return group;
            }

            @Override
            public Set<Name> getExpand() {

                return declaredExpand;
            }

            @Override
            public Map<String, Property.Descriptor> getProperties() {

                return declaredProperties.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Map<String, Link.Descriptor> getLinks() {

                return declaredLinks.entrySet().stream().collect(Collectors.toMap(
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
            public Map<String, Serializable> getExtensions() {

                return extensions;
            }
        };
    }

    @Override
    public boolean equals(final Object other) {

        return qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }
}
