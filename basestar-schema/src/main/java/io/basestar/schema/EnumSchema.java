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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.basestar.expression.Context;
import io.basestar.schema.exception.InvalidTypeException;
import io.basestar.schema.exception.ReservedNameException;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final String name;

    private final int slot;

    /** Text description */

    @Nullable
    private final String description;

    /** Valid values for the enumeration (case sensitive) */

    @Nonnull
    private final List<String> values;

    @Nonnull
    private final Map<String, Object> extensions;

    @Data
    @Accessors(chain = true)
    public static class Builder implements Schema.Builder<String> {

        public static final String TYPE = "enum";

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private String description;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        private List<String> values;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<String, Object> extensions;

        public String getType() {

            return TYPE;
        }

        @Override
        public EnumSchema build() {

            return new EnumSchema(this, name -> null, Schema.anonymousName(), Schema.anonymousSlot());
        }

        @Override
        public EnumSchema build(final Resolver resolver, final String name, final int slot) {

            return new EnumSchema(this, resolver, name, slot);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private EnumSchema(final Builder builder, final Resolver resolver, final String name, final int slot) {

        resolver.constructing(this);
        this.name = name;
        this.slot = slot;
        this.description = builder.getDescription();
        this.values = Nullsafe.immutableCopy(builder.getValues());
        this.extensions = Nullsafe.immutableSortedCopy(builder.getExtensions());
        if(Reserved.isReserved(name)) {
            throw new ReservedNameException(name);
        }
    }

    @Override
    public String create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof String && values.contains(value)) {
            return (String) value;
        } else if(suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Path path, final String after) {

        return Collections.emptySet();
    }
}
