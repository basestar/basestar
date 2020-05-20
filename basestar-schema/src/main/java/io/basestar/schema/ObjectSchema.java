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
import com.google.common.collect.*;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//import io.basestar.util.Key;

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
public class ObjectSchema implements InstanceSchema, Link.Resolver, Index.Resolver, Transient.Resolver {

    @Nonnull
    private final String name;

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
    private final SortedSet<Path> declaredExpand;

    @Nonnull
    private final SortedSet<Path> expand;

    private final boolean concrete;

    @Nonnull
    private final SortedMap<String, Object> extensions;

//    public Set<Path> transientExpand(final Path path, final Set<Path> expand) {
//
//        final Set<Path> transientExpand = new HashSet<>(expand);
//        transientExpand.addAll(InstanceSchema.super.transientExpand(path, expand));
//        final Map<String, Set<Path>> branch = Path.branch(expand);
//        return transientExpand;
//    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "id", "history", "properties", "transients", "links", "indexes", "permissions", "extensions"})
    public static class Builder implements InstanceSchema.Builder {

        public static final String TYPE = "object";

        @Nullable
        private Long version;

        @Nullable
        private String extend;

        @Nullable
        private Id.Builder id;

        @Nullable
        private Boolean concrete;

        @Nullable
        private History history;

        @Nullable
        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Builder> properties;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Transient.Builder> transients;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Link.Builder> links;
//
//        @Nullable
//        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
//        private Map<String, Method.Config> methods;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Index.Builder> indexes;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Permission.Builder> permissions;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        @JsonDeserialize(contentUsing = PathDeserializer.class)
        private Set<Path> expand;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public String getType() {

            return TYPE;
        }

        public Builder setProperty(final String name, final Property.Builder v) {

            properties = Nullsafe.immutableCopyPut(properties, name, v);
            return this;
        }

        public Builder setTransient(final String name, final Transient.Builder v) {

            transients = Nullsafe.immutableCopyPut(transients, name, v);
            return this;
        }

        public Builder setLink(final String name, final Link.Builder v) {

            links = Nullsafe.immutableCopyPut(links, name, v);
            return this;
        }

        public Builder setIndex(final String name, final Index.Builder v) {

            indexes = Nullsafe.immutableCopyPut(indexes, name, v);
            return this;
        }

        public Builder setPermission(final String name, final Permission.Builder v) {

            permissions = Nullsafe.immutableCopyPut(permissions, name, v);
            return this;
        }

        @Override
        public ObjectSchema build(final Resolver resolver, final String name, final int slot) {

            return new ObjectSchema(this, resolver, name, slot);
        }

        @Override
        public ObjectSchema build() {

            return new ObjectSchema(this, name -> null, Schema.anonymousName(), Schema.anonymousSlot());
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public static final SortedMap<String, Use<?>> METADATA_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(Reserved.ID, UseString.DEFAULT)
            .put(Reserved.SCHEMA, UseString.DEFAULT)
            .put(Reserved.VERSION, UseInteger.DEFAULT)
            .put(Reserved.CREATED, UseString.DEFAULT)
            .put(Reserved.UPDATED, UseString.DEFAULT)
            .put(Reserved.HASH, UseString.DEFAULT)
            .build();

    public static final SortedMap<String, Use<?>> REF_SCHEMA = ImmutableSortedMap.<String, Use<?>>orderedBy(Comparator.naturalOrder())
            .put(Reserved.ID, UseString.DEFAULT)
            .build();

    private ObjectSchema(final Builder builder, final Schema.Resolver resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.version = Nullsafe.option(builder.getVersion(), 1L);
        if(builder.getExtend() != null) {
            this.extend = resolver.requireInstanceSchema(builder.getExtend());
        } else {
            this.extend = null;
        }
        this.description = builder.getDescription();
        this.id = builder.getId() == null ? null : builder.getId().build();
        this.history = Nullsafe.option(builder.getHistory(), History.ENABLED);
        this.declaredProperties = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getProperties()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.declaredTransients = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getTransients()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.declaredLinks = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getLinks()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.declaredIndexes = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getIndexes()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.declaredPermissions = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getPermissions()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.declaredExpand = ImmutableSortedSet.copyOf(Nullsafe.option(builder.getExpand()));
        this.concrete = Nullsafe.option(builder.getConcrete(), Boolean.TRUE);
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
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

    public Permission getPermission(final String name) {

        return getPermissions().get(name);
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
    @SuppressWarnings("unchecked")
    public Instance create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return create((Map<String, Object>)value, expand, suppress);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Deprecated
    public Multimap<Path, Instance> refs(final Map<String, Object> object) {

        final Multimap<Path, Instance> results = HashMultimap.create();
        properties.forEach((k, v) -> v.links(object.get(k)).forEach((k2, v2) ->
                results.put(Path.of(v.getName()).with(k2), v2)));
        return results;
    }

    public Instance create(final Map<String, Object> value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        }
        final HashMap<String, Object> result = new HashMap<>(readProperties(value, expand, suppress));
        result.putAll(readMeta(value));
        if(Instance.getSchema(result) == null) {
            Instance.setSchema(result, this.getName());
        }
        if(Instance.getHash(result) == null) {
           Instance.setHash(result, hash(result));
        }
        for(final Map.Entry<String, Object> entry : value.entrySet()) {
            if(entry.getKey().startsWith(Reserved.META_PREFIX)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return new Instance(result);
    }

    private void copyMeta(final Map<String, Object> source, final Map<String, Object> target) {

        METADATA_SCHEMA.keySet().forEach(k -> {
            if(source.containsKey(k)) {
                target.put(k, source.get(k));
            }
        });
    }

    public Instance evaluateProperties(final Context context, final Instance object) {

        final Context thisContext = context.with(VAR_THIS, object);
        final HashMap<String, Object> result = new HashMap<>();
        properties.forEach((k, v) -> result.put(k, v.evaluate(thisContext, object.get(k))));
        copyMeta(object, result);
        result.put(Reserved.HASH, hash(result));
        // Links deliberately not copied, this is only used to prepare an instance for write.
        return new Instance(result);
    }

//    @Override
//    public Instance applyVisibility(final Context context, final Instance object) {
//
//        final Map<String, Object> result = new HashMap<>();
//        Stream.of(allProperties, allTransients, allLinks).forEach(members -> {
//           members.forEach((name, member) -> {
//               if (object.containsKey(name)) {
//                   final Object value = object.get(name);
//                   if (member.isVisible(context, value)) {
//                       result.put(name, member.applyVisibility(context, value));
//                   }
//               }
//           });
//        });
//        copyMeta(object, result);
//        return new Instance(result);
//    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Path path, final Instance after) {

        return validate(context, path, after, after);
    }

    public Set<Constraint.Violation> validate(final Context context, final Instance before, final Instance after) {

        return validate(context, Path.empty(), before, after);
    }

    public Set<Constraint.Violation> validate(final Context context, final Path path, final Instance before, final Instance after) {

        final Set<Constraint.Violation> violations = new HashSet<>();

        if(id != null) {
            violations.addAll(id.validate(path, Instance.getId(after), context));
        }

        violations.addAll(this.getProperties().values().stream()
                .flatMap(v -> v.validate(context, path, before.get(v.getName()), after.get(v.getName())).stream())
                .collect(Collectors.toSet()));

        return violations;
    }

    public String hash(final Map<String, Object> object) {

        try {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream daos = new DataOutputStream(baos)) {
                serializeProperties(object, daos);
                @SuppressWarnings("all")
                final byte[] bytes = Hashing.md5().newHasher().putBytes(baos.toByteArray()).hash().asBytes();
                return BaseEncoding.base64().encode(bytes);
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        final String schema = Instance.getSchema(object);
        final String id = Instance.getId(object);
        final Long version = Instance.getVersion(object);
        final LocalDateTime created = Instance.getCreated(object);
        final LocalDateTime updated = Instance.getUpdated(object);
        final String hash = Instance.getHash(object);
        UseString.DEFAULT.serialize(schema, out);
        UseString.DEFAULT.serialize(id, out);
        UseInteger.DEFAULT.serialize(version, out);
        UseString.DEFAULT.serialize(created == null ? null : created.toString(), out);
        UseString.DEFAULT.serialize(updated == null ? null : updated.toString(), out);
        UseString.DEFAULT.serialize(hash, out);
        serializeProperties(object, out);
    }

    public static Map<String, Object> deserialize(final DataInput in) throws IOException {

        final String schema = Use.deserializeAny(in);
        final String id = Use.deserializeAny(in);
        final Long version = Use.deserializeAny(in);
        final String created = Use.deserializeAny(in);
        final String updated = Use.deserializeAny(in);
        final String hash = Use.deserializeAny(in);

        final Map<String, Object> data = new HashMap<>(InstanceSchema.deserializeProperties(in));
        Instance.setSchema(data, schema);
        Instance.setId(data, id);
        Instance.setVersion(data, version);
        Instance.setCreated(data, created == null ? null : LocalDateTime.parse(created));
        Instance.setUpdated(data, updated == null ? null : LocalDateTime.parse(updated));
        Instance.setHash(data, hash);
        return data;
    }

    public static Instance ref(final String key) {

        return new Instance(ImmutableMap.of(
                Reserved.ID, key
        ));
    }
}
