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
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseView;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.*;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Getter
public class ViewSchema implements LinkableSchema {

    public static final String ID = Reserved.PREFIX + "key";

    public String id(final Map<String, Object> record) {

        return null;
    }

    public interface From extends Serializable {

        Descriptor.From descriptor();

        InferenceContext inferenceContext();

        void collectMaterializationDependencies(Map<Name, LinkableSchema> out);

        void collectDependencies(Map<Name, Schema<?>> out);

        Use<?> typeOfId();

        void validateProperty(Property property);
        
        @Getter
        class FromSql implements From {

            private final String sql;

            private final List<String> primaryKey;

            private final Map<String, From> using;

            public FromSql(final Resolver.Constructing resolver, final Descriptor.From from) {

                this.sql = Nullsafe.require(from.getSql());
                this.primaryKey = Nullsafe.require(from.getPrimaryKey());
                this.using = Immutable.transformValuesSorted(from.getUsing(), (k, v) -> v.build(resolver));
            }

            @Override
            public Descriptor.From descriptor() {

                return new Descriptor.From() {
                    @Override
                    public Name getSchema() {

                        return null;
                    }

                    @Override
                    public List<Sort> getSort() {

                        return null;
                    }

                    @Override
                    public Set<Name> getExpand() {

                        return null;
                    }

                    @Override
                    public String getSql() {

                        return sql;
                    }

                    @Override
                    public Map<String, Descriptor.From> getUsing() {

                        return Immutable.transformValuesSorted(using, (k, v) -> v.descriptor());
                    }

                    @Override
                    public List<String> getPrimaryKey() {

                        return primaryKey;
                    }
                };
            }

            @Override
            public InferenceContext inferenceContext() {

                return InferenceContext.empty();
            }

            @Override
            public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

                using.forEach((k, v) -> v.collectMaterializationDependencies(out));
            }

            @Override
            public void collectDependencies(final Map<Name, Schema<?>> out) {

                using.forEach((k, v) -> v.collectDependencies(out));
            }

            @Override
            public Use<?> typeOfId() {

                return UseBinary.DEFAULT;
            }

            @Override
            public void validateProperty(final Property property) {

                if(property.getExpression() != null) {
                    throw new SchemaValidationException(property.getQualifiedName(), "SQL view properties should not have expressions");
                }
            }
        }

        @Getter
        class FromSchema implements From {

            @Nonnull
            private final LinkableSchema schema;

            @Nonnull
            private final List<Sort> sort;

            @Nonnull
            private final Set<Name> expand;

            public FromSchema(final Resolver.Constructing resolver, final Descriptor.From from) {

                this.schema = resolver.requireLinkableSchema(from.getSchema());
                this.sort = Nullsafe.orDefault(from.getSort());
                this.expand = Nullsafe.orDefault(from.getExpand());
            }

            @Override
            public Descriptor.From descriptor() {

                return new Descriptor.From() {
                    @Override
                    public Name getSchema() {

                        return schema.getQualifiedName();
                    }

                    @Override
                    public List<Sort> getSort() {

                        return sort;
                    }

                    @Override
                    public Set<Name> getExpand() {

                        return expand;
                    }

                    @Override
                    public String getSql() {

                        return null;
                    }

                    @Override
                    public Map<String, Descriptor.From> getUsing() {

                        return null;
                    }

                    @Override
                    public List<String> getPrimaryKey() {

                        return null;
                    }
                };
            }

            @Override
            public InferenceContext inferenceContext() {

                return InferenceContext.from(getSchema());
            }

            @Override
            public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

                final LinkableSchema fromSchema = getSchema();
                if(!out.containsKey(fromSchema.getQualifiedName())) {
                    out.put(fromSchema.getQualifiedName(), fromSchema);
                    fromSchema.collectMaterializationDependencies(getExpand(), out);
                }
            }

            @Override
            public void collectDependencies(final Map<Name, Schema<?>> out) {

                getSchema().collectDependencies(getExpand(), out);
            }

            @Override
            public Use<?> typeOfId() {

                return getSchema().typeOfId();
            }

            @Override
            public void validateProperty(final Property property) {

                if(property.getExpression() == null) {
                    throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
                }
            }
        }

        @Data
        @Accessors(chain = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonPropertyOrder({"schema", "expand", "sort"})
        class Builder implements Descriptor.From {

            @Nullable
            private Name schema;

            @Nullable
            @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
            @JsonSerialize(contentUsing = ToStringSerializer.class)
            @JsonDeserialize(using = AbbrevSetDeserializer.class)
            private Set<Name> expand;

            @Nullable
            @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
            @JsonDeserialize(using = AbbrevListDeserializer.class)
            private List<Sort> sort;

            @Nullable
            private String sql;

            @Nullable
            private Map<String, Descriptor.From> using;

            @Nullable
            @JsonDeserialize(using = AbbrevListDeserializer.class)
            private List<String> primaryKey;

            @JsonCreator
            @SuppressWarnings("unused")
            public static Builder fromSchema(final String schema) {

                return fromSchema(Name.parse(schema));
            }

            public static Builder fromSchema(final Name schema) {

                return new Builder().setSchema(schema);
            }
        }

        static Builder builder() {

            return new Builder();
        }
    }

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends LinkableSchema.Descriptor<ViewSchema> {

        String TYPE = "view";

        @JsonDeserialize(as = ViewSchema.From.Builder.class)
        interface From {

            @JsonInclude(JsonInclude.Include.NON_NULL)
            Name getSchema();

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            List<Sort> getSort();

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Set<Name> getExpand();

            @JsonInclude(JsonInclude.Include.NON_NULL)
            String getSql();

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            Map<String, From> getUsing();

            @JsonInclude(JsonInclude.Include.NON_EMPTY)
            List<String> getPrimaryKey();

            default ViewSchema.From build(final Resolver.Constructing resolver) {

                if(getSql() != null) {
                    return new ViewSchema.From.FromSql(resolver, this);
                } else {
                    return new ViewSchema.From.FromSchema(resolver, this);
                }
            }
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
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Bucketing> bucket;

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
        
        public ViewSchema.Builder setSql(final String sql) {
            
            if(this.from == null) {
                this.from = new ViewSchema.From.Builder();
            }
            ((ViewSchema.From.Builder)this.from).setSql(sql);
            return this;
        }

        @JsonDeserialize(using = AbbrevListDeserializer.class)
        public ViewSchema.Builder setPrimaryKey(final List<String> primaryKey) {

            if(this.from == null) {
                this.from = new ViewSchema.From.Builder();
            }
            ((ViewSchema.From.Builder)this.from).setPrimaryKey(primaryKey);
            return this;
        }

        public ViewSchema.Builder setUsing(final Map<String, From> using) {

            if(this.from == null) {
                this.from = new ViewSchema.From.Builder();
            }
            ((ViewSchema.From.Builder)this.from).setUsing(using);
            return this;
        }
        
        public ViewSchema.Builder addGroup(final String name) {

            group = Immutable.add(group, name);
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
    private final List<Bucketing> declaredBucketing;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    private final boolean isAggregating;

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
        this.from = from.build(resolver);
        this.sort = Immutable.list(descriptor.getSort());
        this.description = descriptor.getDescription();
        this.group = Immutable.list(descriptor.getGroup());
        this.where = descriptor.getWhere();
        final InferenceContext context = this.from.inferenceContext();
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(),
                (k, v) -> v.build(resolver, context, version, qualifiedName.with(k)));
        this.declaredLinks = Immutable.transformValuesSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredPermissions = Immutable.transformValuesSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.declaredBucketing = Immutable.list(descriptor.getBucket());
        this.declaredExpand = Immutable.sortedSet(descriptor.getExpand());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName.toString());
        }
        this.declaredProperties.values().forEach(p -> this.from.validateProperty(p));
        this.isAggregating = getProperties().values().stream().map(Property::getExpression)
                .filter(Objects::nonNull).anyMatch(Expression::isAggregate);
    }

    @Override
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        final Map<String, Object> result = new HashMap<>(readProperties(context, value, expand));
        result.putAll(readMeta(context, value));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if(isAggregating() || isGrouping()) {
            result.computeIfAbsent(ID, k -> {
                final List<Object> values = new ArrayList<>();
                group.forEach(name -> {
                    final Object[] keys = typeOf(Name.of(name)).key(result.get(name));
                    values.addAll(Arrays.asList(keys));
                });
                return BinaryKey.from(values);
            });
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

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        UseBinary.DEFAULT.serialize((Bytes)object.get(ID), out);
        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        final Bytes id = Nullsafe.require(Use.deserializeAny(in));
        final Map<String, Object> data = new HashMap<>(InstanceSchema.deserializeProperties(in));
        data.put(ID, id);
        return new Instance(data);
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

        return ImmutableSortedMap.of(
                ID, typeOfId()
        );
    }

    @Override
    public UseView typeOf() {

        return new UseView(this);
    }


    @Override
    public String id() {

        return ID;
    }

    @Override
    public Use<?> typeOfId() {

        return isGrouping() || isAggregating() ? UseBinary.DEFAULT : from.typeOfId();
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
            from.collectDependencies(out);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
            declaredLinks.forEach((k, v) -> v.collectDependencies(expand, out));
        }
    }

    public boolean isGrouping() {

        return !group.isEmpty();
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

//    public boolean isCoBucketed() {
//
//        final List<Bucketing> viewBucketing = getEffectingBucketing();
//        final List<Bucketing> fromBucketing = from.getSchema().getEffectingBucketing();
//        if(viewBucketing.size() != fromBucketing.size()) {
//            return false;
//        }
//        for(int i = 0; i != viewBucketing.size(); ++i) {
//            if(!isCoBucketed(viewBucketing.get(i), fromBucketing.get(i))) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    private boolean isCoBucketed(final Bucketing viewBucketing, final Bucketing fromBucketing) {
//
//        if(viewBucketing.getCount() != fromBucketing.getCount()) {
//            return false;
//        }
//        if(viewBucketing.getFunction() != fromBucketing.getFunction()) {
//            return false;
//        }
//        final List<Name> viewNames = viewBucketing.getUsing();
//        final List<Name> fromNames = fromBucketing.getUsing();
//        if(viewNames.size() != fromNames.size()) {
//            return false;
//        }
//        for(int i = 0; i != viewNames.size(); ++i) {
//            if(!isSameName(viewNames.get(i), fromNames.get(i))) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    private boolean isSameName(final Name viewName, final Name fromName) {
//
//        final LinkableSchema fromSchema = from.getSchema();
//        final Name fromId = Name.of(fromSchema.id());
//        final Name viewId = Name.of(id());
//        if(fromName.equals(fromId) && viewName.equals(viewId)) {
//            return true;
//        }
//        if(viewName.size() == 1) {
//            final Property viewProp = getProperty(viewName.first(), true);
//            if (viewProp != null) {
//                final Expression viewExpr = viewProp.getExpression();
//                return viewExpr != null && viewExpr.equals(new NameConstant(fromName));
//            }
//        }
//        return false;
//    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {

        LinkableSchema.super.collectMaterializationDependencies(expand, out);
        this.from.collectMaterializationDependencies(out);
    }
}
