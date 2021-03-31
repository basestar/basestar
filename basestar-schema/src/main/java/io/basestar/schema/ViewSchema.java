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
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.jackson.serde.ExpressionDeserializer;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.From;
import io.basestar.schema.from.FromSchema;
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

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends LinkableSchema.Descriptor<ViewSchema> {

        String TYPE = "view";

        @Override
        default String getType() {

            return TYPE;
        }

        Boolean getMaterialized();

        From.Descriptor getFrom();

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
            default From.Descriptor getFrom() {

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
        private From.Descriptor from;

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
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder)this.from).setSql(sql);
            return this;
        }

        @JsonDeserialize(using = AbbrevListDeserializer.class)
        public ViewSchema.Builder setPrimaryKey(final List<String> primaryKey) {

            if(this.from == null) {
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder)this.from).setPrimaryKey(primaryKey);
            return this;
        }

        public ViewSchema.Builder setUsing(final Map<String, From.Descriptor> using) {

            if(this.from == null) {
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder)this.from).setUsing(using);
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

    private final boolean aggregating;

    public static Builder builder() {

        return new Builder();
    }

    private ViewSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.materialized = Nullsafe.orDefault(descriptor.getMaterialized());
        final From.Descriptor from = Nullsafe.require(descriptor.getFrom());
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
        this.declaredProperties.values().forEach(this.from::validateProperty);
        this.aggregating = getProperties().values().stream().map(Property::getExpression)
                .filter(Objects::nonNull).anyMatch(Expression::isAggregate);
    }

    @Override
    public String id(final Map<String, Object> record) {

        final Object key = record.get(ID);
        if(key instanceof String) {
            return (String)key;
        } else if(key instanceof byte[]) {
            return Bytes.valueOf((byte[])key).toBase64();
        } else if(key instanceof Bytes) {
            return ((Bytes)key).toBase64();
        } else {
            throw new IllegalStateException("Missing or invalid " + ID + " (" + key + ")");
        }
    }

    public BinaryKey createId(final Map<String, Object> row) {

        if(isAggregating() || isGrouping()) {
            final List<Object> values = new ArrayList<>();
            group.forEach(name -> {
                final Object[] keys = typeOf(Name.of(name)).key(row.get(name));
                values.addAll(Arrays.asList(keys));
            });
            return BinaryKey.from(values);
        } else {
            return from.id(row);
        }
    }

    @Override
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        final Map<String, Object> result = new HashMap<>(readProperties(context, value, expand));
        result.putAll(readMeta(context, value));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if(isAggregating() || isGrouping()) {
            result.computeIfAbsent(ID, ignored -> createId(result));
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

    public boolean isCoBucketed() {

        return isCompatibleBucketing(getEffectingBucketing());
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        final List<Bucketing> fromBucketing = new ArrayList<>();
        for(final Bucketing bucket : other) {
            final List<Name> using = new ArrayList<>();
            for(final Name name : bucket.getUsing()) {
                final Property property = getProperty(name.first(), true);
                if(property == null) {
                    return false;
                }
                final Expression expression = property.getExpression();
                if(expression == null) {
                    return false;
                }
                if(expression instanceof NameConstant) {
                    using.add(((NameConstant) expression).getName().with(name.withoutFirst()));
                } else {
                    return false;
                }
            }
            fromBucketing.add(new Bucketing(using, bucket.getCount(), bucket.getFunction()));
        }
        return from.isCompatibleBucketing(fromBucketing);
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
