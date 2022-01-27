package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.From;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseQuery;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class QuerySchema implements QueryableSchema {

    @JsonDeserialize(as = QuerySchema.Builder.class)
    public interface Descriptor extends QueryableSchema.Descriptor<QuerySchema> {

        String TYPE = "query";

        @Override
        default String getType() {

            return TYPE;
        }

        From.Descriptor getFrom();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<Argument.Descriptor> getArguments();

        interface Self extends QueryableSchema.Descriptor.Self<QuerySchema>, QuerySchema.Descriptor {

            @Override
            default From.Descriptor getFrom() {

                return self().getFrom().descriptor();
            }

            @Override
            default List<Argument.Descriptor> getArguments() {

                return Immutable.transform(self().getArguments(), Argument::descriptor);
            }
        }

        @Override
        default QuerySchema build(final Namespace namespace, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new QuerySchema(this, resolver, version, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "from", "permissions", "extensions"})
    public static class Builder implements QueryableSchema.Builder<QuerySchema.Builder, QuerySchema>, QuerySchema.Descriptor {

        @Nullable
        private Long version;

        @Nullable
        private String description;

        @Nullable
        private From.Descriptor from;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<Argument.Descriptor> arguments;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Link.Descriptor> links;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Descriptor> permissions;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;

        @SuppressWarnings("unused")
        public QuerySchema.Builder setSql(final String sql) {

            if (this.from == null) {
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder) this.from).setSql(sql);
            return this;
        }

        @SuppressWarnings("unused")
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        public QuerySchema.Builder setPrimaryKey(final List<String> primaryKey) {

            if (this.from == null) {
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder) this.from).setPrimaryKey(primaryKey);
            return this;
        }

        @SuppressWarnings("unused")
        public QuerySchema.Builder setUsing(final Map<String, From.Descriptor> using) {

            if (this.from == null) {
                this.from = new io.basestar.schema.from.From.Builder();
            }
            ((io.basestar.schema.from.From.Builder) this.from).setUsing(using);
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

    /**
     * Description of the schema
     */

    @Nullable
    private final String description;

    @Nonnull
    private final List<Argument> arguments;

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Link> declaredLinks;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    public static Builder builder() {

        return new Builder();
    }

    private QuerySchema(final QuerySchema.Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.from = Optional.ofNullable(descriptor.getFrom()).map(v -> v.build(resolver, Context.init())).orElse(From.EXTERNAL);
        this.arguments = Immutable.transform(descriptor.getArguments(), v -> v.build(resolver));
        this.description = descriptor.getDescription();
        final InferenceContext context = this.from.inferenceContext();
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(),
                (k, v) -> v.build(resolver, context, version, qualifiedName.with(k)));
        this.declaredLinks = Immutable.transformValuesSorted(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredPermissions = Immutable.transformValuesSorted(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if (Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName.toString());
        }
        final List<String> declaredNames = Stream.of(this.declaredProperties, this.declaredLinks)
                .flatMap(v -> v.keySet().stream())
                .collect(Collectors.toList());
        validateFieldNames(declaredNames);
    }

    @Override
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        final Map<String, Object> result = new HashMap<>(readProperties(context, value, expand));
        result.putAll(readMeta(context, value));
        if (Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if (expand != null && !expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            getLinks().forEach((name, link) -> {
                if (value.containsKey(name)) {
                    result.put(name, link.create(context, value.get(name), branches.get(name)));
                }
            });
        }
        return new Instance(result);
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        final Map<String, Object> data = new HashMap<>(InstanceSchema.deserializeProperties(in));
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

        return ImmutableSortedMap.of();
    }

    @Override
    public UseQuery typeOf() {

        return new UseQuery(this);
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

    @Override
    public Member getMember(final String name, final boolean inherited) {

        final Property property = getProperty(name, inherited);
        if (property != null) {
            return property;
        }
        return getLink(name, inherited);
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        if (!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            from.collectDependencies(out);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
            declaredLinks.forEach((k, v) -> v.collectDependencies(expand, out));
        }
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {
        return null;
    }

    @Override
    public List<Sort> sort() {

        return Immutable.list();
    }

    @Override
    public Descriptor descriptor() {

        return (QuerySchema.Descriptor.Self) () -> QuerySchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof QuerySchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out) {

        QueryableSchema.super.collectMaterializationDependencies(expand, out);
        this.from.collectMaterializationDependencies(out);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        return false;
    }
}
