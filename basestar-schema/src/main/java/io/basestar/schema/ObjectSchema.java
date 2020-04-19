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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.basestar.expression.Context;
import io.basestar.schema.exception.ConstraintViolationException;
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
    @JsonProperty("properties")
    private final SortedMap<String, Property> declaredProperties;

    /** Map of property definitions (shares namespace with transients and links) */

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Property> allProperties;

    /** Map of transient definitions (shares namespace with properties and links) */

    @Nonnull
    @JsonProperty("transients")
    private final SortedMap<String, Transient> declaredTransients;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Transient> allTransients;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    @JsonProperty("links")
    private final SortedMap<String, Link> declaredLinks;

    /** Map of link definitions (shares namespace with properties and transients) */

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Link> allLinks;

    /** Map of index definitions */

    @Nonnull
    @JsonProperty("indexes")
    private final SortedMap<String, Index> declaredIndexes;

    /** Map of index definitions */

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Index> allIndexes;

    /** Map of permissions */

    @Nonnull
    @JsonProperty("permissions")
    private final SortedMap<String, Permission> declaredPermissions;

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Permission> allPermissions;

    private final boolean concrete;

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
    public static class Builder implements InstanceSchema.Builder {

        public static final String TYPE = "object";

        @Nullable
        private Long version;

        @Nullable
        private String extend;

        @Nullable
        private Id.Builder id;

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

        private Boolean concrete;

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
        public ObjectSchema build(final Resolver.Cyclic resolver, final String name, final int slot) {

            return new ObjectSchema(this, resolver, name, slot);
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

    private ObjectSchema(final Builder builder, final Schema.Resolver.Cyclic resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.version = Nullsafe.of(builder.getVersion(), 1L);
        if(builder.getExtend() != null) {
            this.extend = resolver.requireInstanceSchema(builder.getExtend());
        } else {
            this.extend = null;
        }
        this.description = builder.getDescription();
        this.id = builder.getId() == null ? null : builder.getId().build();
        this.history = Nullsafe.of(builder.getHistory(), History.ENABLED);
        this.declaredProperties = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getProperties()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.declaredTransients = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getTransients()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.declaredLinks = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getLinks()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.declaredIndexes = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getIndexes()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.declaredPermissions = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getPermissions()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(e.getKey()))));
        this.concrete = Nullsafe.of(builder.getConcrete(), Boolean.TRUE);
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
            this.allProperties = merge(extend.getAllProperties(), declaredProperties);
            if(extend instanceof ObjectSchema) {
                final ObjectSchema objectExtend = (ObjectSchema) extend;
                this.allTransients = merge(objectExtend.getAllTransients(), declaredTransients);
                this.allLinks = merge(objectExtend.getAllLinks(), declaredLinks);
                this.allIndexes = merge(objectExtend.getAllIndexes(), declaredIndexes);
                this.allPermissions = mergePermissions(objectExtend.getAllPermissions(), declaredPermissions);
            } else {
                this.allTransients = declaredTransients;
                this.allLinks = declaredLinks;
                this.allIndexes = declaredIndexes;
                this.allPermissions = declaredPermissions;
            }
        } else {
            this.allProperties = declaredProperties;
            this.allTransients = declaredTransients;
            this.allLinks = declaredLinks;
            this.allIndexes = declaredIndexes;
            this.allPermissions = declaredPermissions;
        }
    }

    private static <T> SortedMap<String, T> merge(final Map<String, T> a, final Map<String, T> b) {

        final SortedMap<String, T> merged = new TreeMap<>();
        merged.putAll(a);
        merged.putAll(b);
        return Collections.unmodifiableSortedMap(merged);
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

//    @Override
//    public Property getProperty(final String name) {
//
//        return properties.get(name);
//    }

//    @Override
//    public Map<String, Property> getDeclaredProperties() {
//
//        return properties;
//    }

//    @Override
//    public Link getLink(final String name) {
//
//        return links.get(name);
//    }
//
//    @Override
//    public Index getIndex(final String name) {
//
//        return indexes.get(name);
//    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(declaredProperties);
        members.putAll(declaredTransients);
        members.putAll(declaredLinks);
        return members;
    }

    @Override
    public Map<String, ? extends Member> getAllMembers() {

        final Map<String, Member> members = new HashMap<>();
        members.putAll(allProperties);
        members.putAll(allTransients);
        members.putAll(allLinks);
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
        final Transient trans = getTransient(name, inherited);
        if(trans != null) {
            return trans;
        }
        return null;
    }

    public Permission getPermission(final String name) {

        return getAllPermissions().get(name);
    }

//    public <T> T getProperty(final Map<String, Object> object, final String name, final Class<T> as) {
//
//        final Property property = requireProperty(name);
//        return property.cast(object.get(name), as);
//    }
//
//    public <T> void setProperty(final Map<String, Object> object, final String name, final T value) {
//
//        final Property property = requireProperty(name);
//        object.put(name, property.create(value));
//    }

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
    public Instance create(final Object value, final boolean expand) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return create((Map<String, Object>)value, expand);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Deprecated
    public Multimap<Path, Instance> refs(final Map<String, Object> object) {

        final Multimap<Path, Instance> results = HashMultimap.create();
        allProperties.forEach((k, v) -> v.links(object.get(k)).forEach((k2, v2) ->
                results.put(Path.of(v.getName()).with(k2), v2)));
        return results;
    }

    public Instance create(final Map<String, Object> value, final boolean expand) {

        final HashMap<String, Object> result = new HashMap<>(readProperties(value, expand));
        Instance.setSchema(result, this.getName());
        result.putAll(readMeta(value));
        if(result.get(Reserved.HASH) == null) {
            result.put(Reserved.HASH, hash(result));
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
        allProperties.forEach((k, v) -> result.put(k, v.evaluate(thisContext, object.get(k))));
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

    public void validate(final Map<String, Object> after, final Context context) {

        validate(Path.empty(), after, after, context);
    }

    public void validate(final Map<String, Object> before, final Map<String, Object> after, final Context context) {

        validate(Path.empty(), before, after, context);
    }

    public void validate(final Path path, final Map<String, Object> before, final Map<String, Object> after, final Context context) {

        final Set<Constraint.Violation> violations = new HashSet<>();

        if(id != null) {
            violations.addAll(id.validate(path, Instance.getId(after), context));
        }

        violations.addAll(getAllProperties().values().stream()
                .flatMap(v -> v.validate(path, before.get(v.getName()), after.get(v.getName()), context).stream())
                .collect(Collectors.toSet()));

        if(!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
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
