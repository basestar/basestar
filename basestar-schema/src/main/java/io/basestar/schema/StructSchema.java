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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
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
    private final SortedMap<String, Property> properties;

    /** Map of property definitions */

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    private final boolean concrete;

    @Nonnull
    private final Map<String, Object> extensions;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "properties", "extensions"})
    public static class Builder implements InstanceSchema.Builder {

        public static final String TYPE = "struct";

        @Nullable
        private Long version;

        @Nullable
        private String extend;

        @Nullable
        private String description;

        @Nullable
        private Boolean concrete;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Builder> properties;

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

        @Override
        public StructSchema build(final Resolver resolver, final String name, final int slot) {

            return new StructSchema(this, resolver, name, slot);
        }

        @Override
        public StructSchema build() {

            return new StructSchema(this, name -> null, Schema.anonymousName(), Schema.anonymousSlot());
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private StructSchema(final Builder builder, final Schema.Resolver resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.version = Nullsafe.option(builder.getVersion(), 1L);
        if(builder.getExtend() != null) {
            this.extend = resolver.requireStructSchema(builder.getExtend());
        } else {
            this.extend = null;
        }
        this.description = builder.getDescription();
        this.declaredProperties = ImmutableSortedMap.copyOf(Nullsafe.option(builder.getProperties()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(resolver, e.getKey()))));
        this.concrete = Nullsafe.option(builder.getConcrete(), Boolean.TRUE);
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
        if(extend != null) {
            final SortedMap<String, Property> merged = new TreeMap<>();
            merged.putAll(extend.getProperties());
            merged.putAll(declaredProperties);
            this.properties = Collections.unmodifiableSortedMap(merged);
        } else {
            this.properties = declaredProperties;
        }
        this.extensions = Nullsafe.option(builder.getExtensions());
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return declaredProperties;
    }

    @Override
    public Map<String, ? extends Member> getAllMembers() {

        return properties;
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        return getProperty(name, inherited);
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

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Path path, final Instance after) {

        return this.getProperties().values().stream()
                .flatMap(v -> v.validate(context, path, after.get(v.getName())).stream())
                .collect(Collectors.toSet());
    }

    public Instance create(final Map<String, Object> value, final boolean expand, final boolean suppress) {

        return new Instance(readProperties(value, expand, suppress));
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        return new Instance(InstanceSchema.deserializeProperties(in));
    }

    @Deprecated
    public Multimap<Path, Instance> refs(final Map<String, Object> object) {

        final Multimap<Path, Instance> results = HashMultimap.create();
        properties.forEach((k, v) -> v.links(object.get(k)).forEach((k2, v2) ->
                results.put(Path.of(v.getName()).with(k2), v2)));
        return results;
    }
}
