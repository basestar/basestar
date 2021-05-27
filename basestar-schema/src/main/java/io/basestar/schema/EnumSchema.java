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
import io.basestar.expression.Context;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.use.UseEnum;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.media.StringSchema;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Enum Schema
 *
 * Enum schemas may be used to constrain strings to a predefined set of values. They are persisted by-value.
 *
 * <strong>Example</strong>
 * <pre>
 * MyEnum:
 *   type: enum
 *   values:
 *   - VALUE1
 *   - VALUE2
 *   - VALUE3
 * </pre>
 */

@Getter
public class EnumSchema implements Schema<String> {

    @Nonnull
    private final Name qualifiedName;

    /**
     * Current version of the schema, defaults to 1
     */

    private final long version;

    private final int slot;

    /** Text description */

    @Nullable
    private final String description;

    /** Valid values for the enumeration (case sensitive) */

    @Nonnull
    private final List<String> values;

    @Nonnull
    private final Map<String, Serializable> extensions;

    @JsonDeserialize(as = Builder.class)
    public interface Descriptor extends Schema.Descriptor<EnumSchema, String> {

        String TYPE = "enum";

        @Override
        default String getType() {

            return TYPE;
        }

        List<String> getValues();

        interface Self extends Schema.Descriptor.Self<EnumSchema, String>, Descriptor {

            @Override
            default List<String> getValues() {

                return self().getValues();
            }
        }

        @Override
        default EnumSchema build(final Namespace namespace, final Resolver.Constructing resolver, final Version version, final Name qualifiedName, final int slot) {

            return new EnumSchema(this, resolver, qualifiedName, slot);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonPropertyOrder({"type", "description", "version", "values", "extensions"})
    public static class Builder implements Schema.Builder<Builder, EnumSchema, String>, Descriptor {

        private Long version;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private String description;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<String> values;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Serializable> extensions;
    }

    public static Builder builder() {

        return new Builder();
    }

    private EnumSchema(final Descriptor descriptor, final Resolver.Constructing resolver, final Name qualifiedName, final int slot) {

        resolver.constructing(qualifiedName, this);
        this.qualifiedName = qualifiedName;
        this.slot = slot;
        this.version = Nullsafe.orDefault(descriptor.getVersion(), 1L);
        this.description = descriptor.getDescription();
        this.values = Immutable.list(descriptor.getValues());
        this.extensions = Immutable.sortedMap(descriptor.getExtensions());
        if(Reserved.isReserved(qualifiedName.last())) {
            throw new ReservedNameException(qualifiedName);
        }
    }

    @Override
    public String create(final ValueContext context, final Object value, final Set<Name> expand) {

        if(value instanceof String && values.contains(value)) {
            return (String) value;
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final String after) {

        return Collections.emptySet();
    }

    @Override
    public Type javaType(final Name name) {

        if(name.isEmpty()) {
            return String.class;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new StringSchema()._enum(values).description(description).name(qualifiedName.toString());
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {

    }

    @Override
    public UseEnum typeOf() {

        return new UseEnum(this);
    }

    @Override
    public String toString(final String value) {

        return Objects.toString(value);
    }

    @Override
    public Descriptor descriptor() {

        return (Descriptor.Self) () -> EnumSchema.this;
    }

    @Override
    public boolean equals(final Object other) {

        return other instanceof EnumSchema && qualifiedNameEquals(other);
    }

    @Override
    public int hashCode() {

        return qualifiedNameHashCode();
    }

    @Override
    public String toString() {

        return getQualifiedName().toString();
    }
}
