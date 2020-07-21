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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import io.basestar.expression.Context;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseStruct;
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
    private final Name qualifiedName;

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

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends InstanceSchema.Descriptor {

        String TYPE = "struct";

        default String type() {

            return TYPE;
        }

        Long getVersion();

        Name getExtend();

        Boolean getConcrete();

        @Override
        default StructSchema build(final Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

            return new StructSchema(this, resolver, qualifiedName, slot);
        }

        @Override
        default StructSchema build() {

            return build(Resolver.Constructing.ANONYMOUS, Schema.anonymousQualifiedName(), Schema.anonymousSlot());
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "properties", "extensions"})
    public static class Builder implements InstanceSchema.Builder, Descriptor {

        @Nullable
        private Long version;

        @Nullable
        private Name extend;

        @Nullable
        private String description;

        @Nullable
        private Boolean concrete;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public String getType() {

            return TYPE;
        }

        public Builder setProperty(final String name, final Property.Descriptor v) {

            properties = Nullsafe.immutableCopyPut(properties, name, v);
            return this;
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private StructSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

        resolver.constructing(this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.option(descriptor.getVersion(), 1L);
        if(descriptor.getExtend() != null) {
            this.extend = resolver.requireStructSchema(descriptor.getExtend());
        } else {
            this.extend = null;
        }
        this.description = descriptor.getDescription();
        this.declaredProperties = Nullsafe.immutableSortedCopy(descriptor.getProperties(), (k, v) -> v.build(resolver, qualifiedName.with(k)));
        this.declaredProperties.forEach((k, v) -> {
            if(v.isImmutable()) {
                throw new SchemaValidationException(qualifiedName, "Struct types cannot have immutable properties");
            }
            if(v.getExpression() != null) {
                throw new SchemaValidationException(qualifiedName,"Struct types cannot have properties with expressions");
            }
            if(v.getVisibility() != null) {
                throw new SchemaValidationException(qualifiedName,"Struct types cannot have properties with custom visibility");
            }
        });
        this.concrete = Nullsafe.option(descriptor.getConcrete(), Boolean.TRUE);
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
        if(extend != null) {
            final SortedMap<String, Property> merged = new TreeMap<>();
            merged.putAll(extend.getProperties());
            merged.putAll(declaredProperties);
            this.properties = Collections.unmodifiableSortedMap(merged);
        } else {
            this.properties = declaredProperties;
        }
        this.extensions = Nullsafe.option(descriptor.getExtensions());
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

    @Override
    public UseStruct use() {

        return new UseStruct(this);
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return declaredProperties;
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        return properties;
    }

    @Override
    public Member getMember(final String name, final boolean inherited) {

        return getProperty(name, inherited);
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {

        return this.getProperties().values().stream()
                .flatMap(v -> v.validate(context, name, after.get(v.getName())).stream())
                .collect(Collectors.toSet());
    }

    @Override
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
    public Multimap<Name, Instance> refs(final Map<String, Object> object) {

        final Multimap<Name, Instance> results = HashMultimap.create();
        properties.forEach((k, v) -> v.links(object.get(k)).entries().forEach(e ->
                results.put(Name.of(v.getName()).with(e.getKey()), e.getValue())));
        return results;
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            if(extend != null) {
                extend.collectDependencies(expand, out);
            }
            out.put(qualifiedName, this);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
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
            public Boolean getConcrete() {

                return concrete;
            }

            @Override
            public Map<String, Property.Descriptor> getProperties() {

                return declaredProperties.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().descriptor()
                ));
            }

            @Nullable
            @Override
            public String getDescription() {

                return description;
            }

            @Override
            public Map<String, Object> getExtensions() {

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
