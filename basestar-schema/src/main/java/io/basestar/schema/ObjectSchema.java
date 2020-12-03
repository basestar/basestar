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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.NameDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Object Schema
 *
 * Objects are persisted by reference, and may be polymorphic.
 *
 * Objects may contain properties, transients and links, these member types share the same
 * namespace, meaning you cannot define a property and a transient or link with the same name.
 *
 * <strong>Example</strong>
 * <pre>
 * MyObject:
 *   type: object
 *   properties:
 *      myProperty1:
 *          type: string
 * </pre>
 */

@Getter
@Accessors(chain = true)
public class ObjectSchema implements LinkableSchema, Index.Resolver, Transient.Resolver, Permission.Resolver {

    public static final String ID = "id";

    public static final String SCHEMA = "schema";

    public static final String CREATED = "created";

    public static final String UPDATED = "updated";

    public static final String VERSION = "version";

    public static final String HASH = "hash";

    public static final Name ID_NAME = Name.of(ID);

    public static final SortedMap<String, Use<?>> METADATA_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(ID, UseString.DEFAULT)
            .put(SCHEMA, UseString.DEFAULT)
            .put(VERSION, UseInteger.DEFAULT)
            .put(CREATED, UseDateTime.DEFAULT)
            .put(UPDATED, UseDateTime.DEFAULT)
            .put(HASH, UseString.DEFAULT)
            .build();

    public static final SortedMap<String, Use<?>> REF_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(ID, UseString.DEFAULT)
            .build();

    @Nonnull
    private final Name qualifiedName;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    /** Parent schema, may be another object schema or a struct schema */

    @Nullable
    private final InstanceSchema extend;

    /** Id configuration */

    @Nullable
    private final Id id;

    /** History configuration */

    @Nonnull
    private final History history;

    /** Description of the schema */

    @Nullable
    private final String description;

    /** Map of property definitions (shares namespace with transients and links) */

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    /** Map of property definitions (shares namespace with transients and links) */

    @Nonnull
    private final SortedMap<String, Property> properties;

    /** Map of transient definitions (shares namespace with properties and links) */

    @Nonnull
    private final SortedMap<String, Transient> declaredTransients;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    private final SortedMap<String, Transient> transients;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    private final SortedMap<String, Link> declaredLinks;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    private final SortedMap<String, Link> links;

    /** Map of index definitions */

    @Nonnull
    private final SortedMap<String, Index> declaredIndexes;

    /** Map of index definitions */

    @Nonnull
    private final SortedMap<String, Index> indexes;

    /** Map of permissions */

    @Nonnull
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    private final SortedMap<String, Permission> permissions;

    @Nonnull
    private final List<Constraint> constraints;

    @Nonnull
    private final SortedSet<Name> declaredExpand;

    @Nonnull
    private final SortedSet<Name> expand;

    private final boolean concrete;

    private final boolean readonly;

    @Nonnull
    private final SortedMap<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends LinkableSchema.Descriptor {

        String TYPE = "object";

        @Override
        default String getType() {

            return TYPE;
        }

        Long getVersion();

        Name getExtend();

        Id.Descriptor getId();

        Boolean getConcrete();

        Boolean getReadonly();

        History getHistory();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Transient.Descriptor> getTransients();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Index.Descriptor> getIndexes();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        List<? extends Constraint> getConstraints();

        @Override
        default ObjectSchema build(final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new ObjectSchema(this, resolver, version, qualifiedName, slot);
        }

        @Override
        default ObjectSchema build(final Name qualifiedName) {

            return build(Resolver.Constructing.ANONYMOUS, Version.CURRENT, qualifiedName, Schema.anonymousSlot());
        }

        @Override
        default ObjectSchema build() {

            return build(Schema.anonymousQualifiedName());
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "id", "history", "properties", "transients", "links", "indexes", "permissions", "extensions"})
    public static class Builder implements InstanceSchema.Builder, Descriptor, Link.Resolver.Builder, Transient.Resolver.Builder {

        private Long version;

        @Nullable
        private Name extend;

        @Nullable
        private Id.Builder id;

        @Nullable
        private Boolean concrete;

        @Nullable
        private Boolean readonly;

        @Nullable
        private History history;

        @Nullable
        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Transient.Descriptor> transients;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Link.Descriptor> links;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Index.Descriptor> indexes;

        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<? extends Constraint> constraints;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Descriptor> permissions;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(contentUsing = NameDeserializer.class)
        private Set<Name> expand;

        @Nullable
        private Map<String, Serializable> extensions;

        @Override
        public Builder setProperty(final String name, final Property.Descriptor v) {

            properties = Nullsafe.immutableCopyPut(properties, name, v);
            return this;
        }

        @Override
        public Builder setTransient(final String name, final Transient.Descriptor v) {

            transients = Nullsafe.immutableCopyPut(transients, name, v);
            return this;
        }

        @Override
        public Builder setLink(final String name, final Link.Descriptor v) {

            links = Nullsafe.immutableCopyPut(links, name, v);
            return this;
        }

        public Builder setIndex(final String name, final Index.Descriptor v) {

            indexes = Nullsafe.immutableCopyPut(indexes, name, v);
            return this;
        }

        public Builder setPermission(final String name, final Permission.Descriptor v) {

            permissions = Nullsafe.immutableCopyPut(permissions, name, v);
            return this;
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private ObjectSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        if(descriptor.getExtend() != null) {
            this.extend = resolver.requireInstanceSchema(descriptor.getExtend());
        } else {
            this.extend = null;
        }
        this.description = descriptor.getDescription();
        this.id = descriptor.getId() == null ? null : descriptor.getId().build(qualifiedName.with(ID));
        this.history = Nullsafe.orDefault(descriptor.getHistory(), History.ENABLED);
        this.declaredProperties = Nullsafe.immutableSortedCopy(descriptor.getProperties(), (k, v) -> v.build(resolver, version, qualifiedName.with(k)));
        this.declaredTransients = Nullsafe.immutableSortedCopy(descriptor.getTransients(), (k, v) -> v.build(qualifiedName.with(k)));
        this.declaredLinks = Nullsafe.immutableSortedCopy(descriptor.getLinks(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredIndexes = Nullsafe.immutableSortedCopy(descriptor.getIndexes(), (k, v) -> v.build(qualifiedName.with(k)));
        this.constraints = Nullsafe.immutableCopy(descriptor.getConstraints());
        this.declaredPermissions = Nullsafe.immutableSortedCopy(descriptor.getPermissions(), (k, v) -> v.build(k));
        this.declaredExpand = Nullsafe.immutableSortedCopy(descriptor.getExpand());
        this.concrete = Nullsafe.orDefault(descriptor.getConcrete(), Boolean.TRUE);
        this.readonly = Nullsafe.orDefault(descriptor.getReadonly());
        this.extensions = Nullsafe.immutableSortedCopy(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        Stream.of(this.declaredProperties, this.declaredLinks, this.declaredTransients)
                .flatMap(v -> v.keySet().stream()).forEach(k -> {
                    if(METADATA_SCHEMA.containsKey(k)) {
                        throw new ReservedNameException(k);
                    }
        });
        if(extend != null) {
            this.properties = merge(extend.getProperties(), declaredProperties);
            if(extend instanceof ObjectSchema) {
                final ObjectSchema objectExtend = (ObjectSchema) extend;
                this.transients = merge(objectExtend.getTransients(), declaredTransients);
                this.links = merge(objectExtend.getLinks(), declaredLinks);
                this.indexes = merge(objectExtend.getIndexes(), declaredIndexes);
                this.permissions = mergePermissions(objectExtend.getPermissions(), declaredPermissions);
                this.expand = merge(objectExtend.getExpand(), declaredExpand);
            } else {
                this.transients = declaredTransients;
                this.links = declaredLinks;
                this.indexes = declaredIndexes;
                this.permissions = declaredPermissions;
                this.expand = declaredExpand;
            }
        } else {
            this.properties = declaredProperties;
            this.transients = declaredTransients;
            this.links = declaredLinks;
            this.indexes = declaredIndexes;
            this.permissions = declaredPermissions;
            this.expand = declaredExpand;
        }
    }

    private static <T> SortedMap<String, T> merge(final Map<String, T> a, final Map<String, T> b) {

        final SortedMap<String, T> merged = new TreeMap<>();
        merged.putAll(a);
        merged.putAll(b);
        return Collections.unmodifiableSortedMap(merged);
    }

    private static <T extends Comparable<T>> SortedSet<T> merge(final Set<T> a, final Set<T> b) {

        final SortedSet<T> merged = new TreeSet<>();
        merged.addAll(a);
        merged.addAll(b);
        return Collections.unmodifiableSortedSet(merged);
    }

    private static SortedMap<String, Permission> mergePermissions(final Map<String, Permission> a, final Map<String, Permission> b) {

        final SortedMap<String, Permission> merged = new TreeMap<>(a);
        b.forEach((k, v) -> {
            if(merged.containsKey(k)) {
                merged.put(k, merged.get(k).merge(v));
            } else {
                merged.put(k, v);
            }
        });
        return Collections.unmodifiableSortedMap(merged);
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return METADATA_SCHEMA;
    }

    @Override
    public UseObject typeOf() {

        return new UseObject(this);
    }

    @Override
    public String id() {

        return ID;
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(declaredProperties);
        members.putAll(declaredTransients);
        members.putAll(declaredLinks);
        return members;
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(properties);
        members.putAll(transients);
        members.putAll(links);
        return members;
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        final Property property = getProperty(name, inherited);
        if(property != null) {
            return property;
        }
        final Link link = getLink(name, inherited);
        if(link != null) {
            return link;
        }
        return getTransient(name, inherited);
    }

    public static Map<String, Object> readMeta(final Map<String, Object> object) {

        final HashMap<String, Object> result = new HashMap<>();
        METADATA_SCHEMA.keySet().forEach(k -> {
            if(object.containsKey(k)) {
                result.put(k, object.get(k));
            }
        });
        return Collections.unmodifiableMap(result);
    }

    @Override
    public Instance create(final Map<String, Object> value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        }
        final HashMap<String, Object> result = new HashMap<>(readProperties(value, expand, suppress));
        result.putAll(readMeta(value, suppress));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getQualifiedName());
        }
        if(Instance.getHash(result) == null) {
           Instance.setHash(result, hash(result));
        }
        for(final Map.Entry<String, Object> entry : value.entrySet()) {
            if(entry.getKey().startsWith(Reserved.META_PREFIX)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        if(expand != null && !expand.isEmpty()) {
            final Map<String, Set<Name>> branches = Name.branch(expand);
            Stream.of(links, transients).forEach(members -> members.forEach((name, link) -> {
                if(value.containsKey(name)) {
                    result.put(name, link.create(value.get(name), branches.get(name), suppress));
                }
            }));
        }
        return new Instance(result);
    }

    public static Map<String, Object> copyMeta(final Map<String, Object> source) {

        final Map<String, Object> target = new HashMap<>();
        copyMeta(source, target);
        return Collections.unmodifiableMap(target);
    }

    public static void copyMeta(final Map<String, Object> source, final Map<String, Object> target) {

        source.forEach((k, v) -> {
            if(METADATA_SCHEMA.containsKey(k) || Reserved.isMeta(k)) {
                target.put(k, v);
            }
        });
    }

    public Instance evaluateProperties(final Context context, final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Context thisContext = context.with(VAR_THIS, object);
        final HashMap<String, Object> result = new HashMap<>();
        properties.forEach((k, v) -> result.put(k, v.evaluate(thisContext, branches.get(k), object.get(k))));
        copyMeta(object, result);
        result.put(HASH, hash(result));
        // Links deliberately not copied, this is only used to prepare an instance for write.
        return new Instance(result);
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {

        return validate(context, name, after, after);
    }

    public Set<Constraint.Violation> validate(final Context context, final Instance before, final Instance after) {

        return validate(context, Name.empty(), before, after);
    }

    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance before, final Instance after) {

        final Set<Constraint.Violation> violations = new HashSet<>();

        if(id != null) {
            violations.addAll(id.validate(name, Instance.getId(after), context));
        }

        violations.addAll(this.getConstraints().stream()
                .flatMap(v -> v.violations(new UseObject(this), context, name, after).stream())
                .collect(Collectors.toSet()));

        violations.addAll(this.getProperties().values().stream()
                .flatMap(v -> v.validate(context, name, before.get(v.getName()), after.get(v.getName())).stream())
                .collect(Collectors.toSet()));

        return violations;
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        final Name schema = Instance.getSchema(object);
        final String id = Instance.getId(object);
        final Long version = Instance.getVersion(object);
        final Instant created = Instance.getCreated(object);
        final Instant updated = Instance.getUpdated(object);
        final String hash = Instance.getHash(object);
        UseString.DEFAULT.serialize(schema == null ? null : schema.toString(), out);
        UseString.DEFAULT.serialize(id, out);
        UseInteger.DEFAULT.serialize(version, out);
        UseDateTime.DEFAULT.serialize(created, out);
        UseDateTime.DEFAULT.serialize(updated, out);
        UseString.DEFAULT.serialize(hash, out);
        serializeProperties(object, out);
    }

    public static Map<String, Object> deserialize(final DataInput in) throws IOException {

        final String schema = Use.deserializeAny(in);
        final String id = Use.deserializeAny(in);
        final Long version = Use.deserializeAny(in);
        final Instant created = Use.deserializeAny(in);
        final Instant updated = Use.deserializeAny(in);
        final String hash = Use.deserializeAny(in);

        final Map<String, Object> data = new HashMap<>(InstanceSchema.deserializeProperties(in));
        Instance.setSchema(data, schema == null ? null : Name.parse(schema));
        Instance.setId(data, id);
        Instance.setVersion(data, version);
        Instance.setCreated(data, created);
        Instance.setUpdated(data, updated);
        Instance.setHash(data, hash);
        return data;
    }

    public static Instance ref(final String key) {

        return new Instance(ImmutableMap.of(
                ID, key
        ));
    }

    public static Instance ref(final String key, final Long version) {

        return new Instance(ImmutableMap.of(
                ID, key,
                VERSION, version
        ));
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            if(extend != null) {
                extend.collectDependencies(expand, out);
            }
            out.put(qualifiedName, this);
            final Map<String, Set<Name>> branches = Name.branch(expand);
            declaredProperties.forEach((k, v) -> v.collectDependencies(branches.get(k), out));
            Stream.of(declaredLinks, declaredTransients).forEach(members -> members.forEach((k, v) -> {
                final Set<Name> branch = branches.get(k);
                if(branch != null) {
                    v.collectDependencies(branch, out);
                }
            }));
        }
    }

    @Override
    public Descriptor descriptor() {

        return new Descriptor() {

            @Override
            public Long getVersion() {

                return version;
            }

            @Override
            public Name getExtend() {

                return extend == null ? null : extend.getQualifiedName();
            }

            @Override
            public Id.Descriptor getId() {

                return id == null ? null : id.descriptor();
            }

            @Override
            public Boolean getConcrete() {

                return concrete;
            }

            @Override
            public Boolean getReadonly() {

                return readonly;
            }

            @Override
            public History getHistory() {

                return history;
            }

            @Override
            public Map<String, Property.Descriptor> getProperties() {

                return declaredProperties.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Map<String, Transient.Descriptor> getTransients() {

                return declaredTransients.entrySet().stream().collect(Collectors.toMap(
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
            public Map<String, Index.Descriptor> getIndexes() {

                return declaredIndexes.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public List<? extends Constraint> getConstraints() {

                return constraints;
            }

            @Override
            public Map<String, Permission.Descriptor> getPermissions() {

                return declaredPermissions.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
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

        return other instanceof ObjectSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    public void validateObject(final String id, final Instance after) {

        if(!id.equals(Instance.getId(after))) {
            throw new IllegalStateException("Instance validation failed: id mismatch");
        }
        final Name schemaName = Instance.getSchema(after);
        if(schemaName == null || !isSubclassOf(schemaName)) {
            throw new IllegalStateException("Instance validation failed: schema mismatch");
        }
    }
}
