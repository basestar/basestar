package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.use.Use;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

//import io.basestar.util.Key;

/**
 * Struct Schema
 *
 * Struct schemas may have properties, but no transients, links etc. A struct is persisted by value.
 *
 * When a struct type is referenced as a property type, it is static (non-polymorphic), that is
 * when persisting only the fields of the declared type can be stored.
 *
 * <strong>Example</strong>
 * <pre>
 * MyStruct:
 *   type: struct
 *   properties:
 *      myProperty1:
 *          type: string
 * </pre>
 */

@Getter
@Accessors(chain = true)
public class StructSchema implements InstanceSchema {

    @Nonnull
    private final String name;

    private final int slot;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    /** Parent schema, may be another struct schema only */

    @Nullable
    private final StructSchema extend;

    /** Description of the schema */

    @Nullable
    private final String description;

    /** Map of property definitions */

    @Nonnull
    @JsonIgnore
    private final SortedMap<String, Property> allProperties;

    /** Map of property definitions */

    @Nonnull
    @JsonProperty("properties")
    private final SortedMap<String, Property> declaredProperties;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements InstanceSchema.Builder {

        public static final String TYPE = "struct";

        @Nullable
        private Long version;

        @Nullable
        private String extend;

        @Nullable
        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Builder> properties;

        public Builder setProperty(final String name, final Property.Builder v) {

            properties = Nullsafe.immutableCopyPut(properties, name, v);
            return this;
        }

        @Override
        public StructSchema build(final Resolver.Cyclic resolver, final String name, final int slot) {

            return new StructSchema(this, resolver, name, slot);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private StructSchema(final Builder builder, final Schema.Resolver.Cyclic resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.version = Nullsafe.of(builder.getVersion(), 1L);
        if(builder.getExtend() != null) {
            this.extend = resolver.requireStructSchema(builder.getExtend());
        } else {
            this.extend = null;
        }
        this.description = builder.getDescription();
        this.declaredProperties = ImmutableSortedMap.copyOf(Nullsafe.of(builder.getProperties()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
        if(extend != null) {
            final SortedMap<String, Property> merged = new TreeMap<>();
            merged.putAll(extend.getAllProperties());
            merged.putAll(declaredProperties);
            this.allProperties = Collections.unmodifiableSortedMap(merged);
        } else {
            this.allProperties = declaredProperties;
        }
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

//    @Override
//    public Property getProperty(final String name) {
//
//        return properties.get(name);
//    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return declaredProperties;
    }

    @Override
    public Map<String, ? extends Member> getAllMembers() {

        return allProperties;
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        return getProperty(name, inherited);
    }

//    public Map<String, Object> readProperties(final Map<String, Object> object) {
//
//        final Map<String, Object> result = new HashMap<>();
//        getProperties().forEach((k, v) -> result.put(k, v.create(object.get(k))));
//        return Collections.unmodifiableMap(result);
//    }

    @SuppressWarnings("unchecked")
    public Instance create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Map) {
            return create((Map<String, Object>)value);
        } else {
            throw new InvalidTypeException();
        }
    }

    public Instance create(final Map<String, Object> value) {

        return new Instance(readProperties(value));
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        return new Instance(InstanceSchema.deserializeProperties(in));
    }

    @Deprecated
    public Set<Path> requiredExpand(final Set<Path> paths) {

        final Set<Path> result = new HashSet<>();
        for (final Map.Entry<String, Set<Path>> branch : Path.branch(paths).entrySet()) {
            final Member member = getMember(branch.getKey(), true);
            if(member != null) {
                for(final Path tail : member.requireExpand(branch.getValue())) {
                    result.add(Path.of(branch.getKey()).with(tail));
                }
            }
        }
        return Path.simplify(result);
    }

    @Deprecated
    public Multimap<Path, Instance> refs(final Map<String, Object> object) {

        final Multimap<Path, Instance> results = HashMultimap.create();
        allProperties.forEach((k, v) -> v.links(object.get(k)).forEach((k2, v2) ->
                results.put(Path.of(v.getName()).with(k2), v2)));
        return results;
    }
}
