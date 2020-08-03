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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.basestar.expression.Context;
import io.basestar.schema.exception.MissingSchemaException;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * Schema
 *
 * Base type for schema definitions
 *
 * @param <T>
 */

public interface Schema<T> extends Named, Described, Serializable, Extendable {

    Name ANONYMOUS_NAME = Name.of(Reserved.PREFIX + "anon");

    String VAR_THIS = "this";

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ObjectSchema.Builder.class)
    @JsonSubTypes({
            @JsonSubTypes.Type(name = EnumSchema.Builder.TYPE, value = EnumSchema.Builder.class),
            @JsonSubTypes.Type(name = StructSchema.Builder.TYPE, value = StructSchema.Builder.class),
            @JsonSubTypes.Type(name = ObjectSchema.Builder.TYPE, value = ObjectSchema.Builder.class),
            @JsonSubTypes.Type(name = ViewSchema.Builder.TYPE, value = ViewSchema.Builder.class)
    })
    interface Descriptor<T> extends Described, Extendable {

        String type();

        Long getVersion();

        Schema<T> build(Resolver.Constructing resolver, Name qualifiedName, int slot);

        Schema<T> build();
    }

    default Name getQualifiedPackageName() {

        return getQualifiedName().withoutLast();
    }

    default String getPackageName() {

        return getPackageName(Character.toString(Name.DELIMITER));
    }

    default String getPackageName(final String delimiter) {

        final Name qualifiedName = getQualifiedPackageName();
        return qualifiedName.isEmpty() ? null : qualifiedName.toString(delimiter);
    }

    interface Builder<T> extends Descriptor<T> {

    }

    default T create(final Object value) {

        return create(value, Collections.emptySet(), false);
    }

    T create(Object value, Set<Name> expand, boolean suppress);

    int getSlot();

    static Name anonymousQualifiedName() {

        return ANONYMOUS_NAME.with(UUID.randomUUID().toString());
    }

    static int anonymousSlot() {

        return -1;
    }

    default boolean isAnonymous() {

        return getSlot() == anonymousSlot();
    }

    default Set<Constraint.Violation> validate(final Context context, final T after) {

        return validate(context, Name.empty(), after);
    }

    Set<Constraint.Violation> validate(Context context, Name name, T after);

    io.swagger.v3.oas.models.media.Schema<?> openApi();

    Descriptor<T> descriptor();

    Use<T> use();

    default Map<Name, Schema<?>> dependencies() {

        return dependencies(Collections.emptySet());
    }

    default Map<Name, Schema<?>> dependencies(final Set<Name> expand) {

        final Map<Name, Schema<?>> dependencies = new HashMap<>();
        collectDependencies(expand, dependencies);
        return dependencies;
    }

    void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out);

    interface Resolver {

        interface Constructing extends Resolver {

            // 'Magic' method to handles cycles in namespace builder, instance under construction
            // must call resolver.constructing(this); as first constructor line.

            void constructing(final Schema<?> schema);

            // Convenience for anonymous construction

            Constructing ANONYMOUS = new Constructing() {

                @Override
                public void constructing(final Schema<?> schema) {

                }

                @Nullable
                @Override
                public Schema<?> getSchema(final Name qualifiedName) {
                    return null;
                }
            };
        }

        @Nullable
        Schema<?> getSchema(Name qualifiedName);

        @Nonnull
        default Schema<?> requireSchema(final Name qualifiedName) {

            final Schema<?> result = getSchema(qualifiedName);
            if(result == null) {
                throw new MissingSchemaException(qualifiedName);
            } else {
                return result;
            }
        }

        default Schema<?> requireSchema(final String name) {

            return requireSchema(Name.parse(name));
        }

        @Nonnull
        default InstanceSchema requireInstanceSchema(final Name qualifiedName) {

            final Schema<?> schema = requireSchema(qualifiedName);
            if(schema instanceof InstanceSchema) {
                return (InstanceSchema)schema;
            } else {
                throw new IllegalStateException(qualifiedName + " is not an instance schema");
            }
        }

        default InstanceSchema requireInstanceSchema(final String name) {

            return requireInstanceSchema(Name.parse(name));
        }

        @Nonnull
        default ObjectSchema requireObjectSchema(final Name qualifiedName) {

            final Schema<?> schema = requireSchema(qualifiedName);
            if (schema instanceof ObjectSchema) {
                return (ObjectSchema) schema;
            } else {
                throw new IllegalStateException(qualifiedName + " is not an object schema");
            }
        }

        default ObjectSchema requireObjectSchema(final String name) {

            return requireObjectSchema(Name.parse(name));
        }

        @Nonnull
        default StructSchema requireStructSchema(final Name qualifiedName) {

            final Schema<?> schema = requireSchema(qualifiedName);
            if(schema instanceof StructSchema) {
                return (StructSchema)schema;
            } else {
                throw new IllegalStateException(qualifiedName + " is not a struct schema");
            }
        }

        default StructSchema requireStructSchema(final String name) {

            return requireStructSchema(Name.parse(name));
        }

        @Nonnull
        default EnumSchema requireEnumSchema(final Name qualifiedName) {

            final Schema<?> schema = requireSchema(qualifiedName);
            if(schema instanceof EnumSchema) {
                return (EnumSchema)schema;
            } else {
                throw new IllegalStateException(qualifiedName + " is not an enum schema");
            }
        }

        default EnumSchema requireEnumSchema(final String name) {

            return requireEnumSchema(Name.parse(name));
        }

        @Nonnull
        default ViewSchema requireViewSchema(final Name qualifiedName) {

            final Schema<?> schema = requireSchema(qualifiedName);
            if (schema instanceof ViewSchema) {
                return (ViewSchema) schema;
            } else {
                throw new IllegalStateException(qualifiedName + " is not a view schema");
            }
        }

        default ViewSchema requireViewSchema(final String name) {

            return requireViewSchema(Name.parse(name));
        }
    }
}
