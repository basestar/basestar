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
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseStruct;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

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

    /** Description of the schema */

    @Nullable
    private final String description;

    /** Map of property definitions */

    @Nonnull
    private final SortedMap<String, Property> declaredProperties;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends InstanceSchema.Descriptor<StructSchema> {

        String TYPE = "struct";

        @Override
        default String getType() {

            return TYPE;
        }

        interface Self extends InstanceSchema.Descriptor.Self<StructSchema>, Descriptor {

        }

        @Override
        default StructSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new StructSchema(this, resolver, version, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "description", "version", "extend", "concrete", "properties", "extensions"})
    public static class Builder implements InstanceSchema.Builder<Builder, StructSchema>, Descriptor {

        @Nullable
        private Long version;

        @Nullable
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        private List<Name> extend;

        @Nullable
        private String description;

        @Nullable
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private Map<String, Property.Descriptor> properties;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private StructSchema(final Descriptor descriptor, final Schema.Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.declaredProperties = Immutable.transformValuesSorted(descriptor.getProperties(), (k, v) -> v.build(resolver, version, qualifiedName.with(k)));
        this.declaredProperties.forEach((k, v) -> {
            if(v.isImmutable()) {
                throw new SchemaValidationException(qualifiedName, "Struct types cannot have immutable properties");
            }
            if(v.getExpression() != null) {
                throw new SchemaValidationException(qualifiedName, "Struct types cannot have properties with expressions");
            }
            if(v.getVisibility() != null) {
                throw new SchemaValidationException(qualifiedName, "Struct types cannot have properties with custom visibility");
            }
        });
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }

        this.extensions = Nullsafe.orDefault(descriptor.getExtensions());
    }

    @Override
    public SortedMap<String, Use<?>> metadataSchema() {

        return ImmutableSortedMap.of();
    }

    @Override
    public UseStruct typeOf() {

        return new UseStruct(this);
    }

    @Override
    public boolean isConcrete() {

        return true;
    }

    @Override
    public Map<String, Property> getProperties() {

        return getDeclaredProperties();
    }

    @Override
    public Map<String, ? extends Member> getDeclaredMembers() {

        return declaredProperties;
    }

    @Override
    public Map<String, ? extends Member> getMembers() {

        return getProperties();
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
    public Instance create(final ValueContext context, final Map<String, Object> value, final Set<Name> expand) {

        return new Instance(readProperties(context, value, expand));
    }

    public void serialize(final Map<String, Object> object, final DataOutput out) throws IOException {

        serializeProperties(object, out);
    }

    public static Instance deserialize(final DataInput in) throws IOException {

        return new Instance(InstanceSchema.deserializeProperties(in));
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        if(!out.containsKey(qualifiedName)) {
            out.put(qualifiedName, this);
            declaredProperties.forEach((k, v) -> v.collectDependencies(expand, out));
        }
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> StructSchema.this;
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        if(name.isEmpty()) {
            return false;
        }
        final Member member = requireMember(name.first(), true);
        return member.isCompatibleBucketing(other, name.withoutFirst());
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof StructSchema &&qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    public static StructSchema from(final Name qualifiedName, final Map<String, ?> schema) {

        return StructSchema.builder()
                .setProperties(schema.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> property(e.getValue()))))
                .build(qualifiedName);
    }

    public static StructSchema from(final Map<String, ?> schema) {

        return from(Schema.anonymousQualifiedName(), schema);
    }

    private static Property.Descriptor property(final Object config) {

        if(config instanceof Property.Descriptor) {
            return (Property.Descriptor)config;
        } else if(config instanceof Property) {
            return ((Property)config).descriptor();
        } else if(config instanceof Use<?>) {
            return Property.builder()
                    .setType((Use<?>)config);
        } else {
            return Property.builder()
                    .setType(Use.fromConfig(config));
        }
    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }
}
