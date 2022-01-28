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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import io.basestar.schema.exception.MissingSchemaException;
import io.basestar.util.Name;
import io.basestar.util.Text;
import io.basestar.util.Warnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * Schema
 * <p>
 * Base type for schema definitions
 */

@SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
public interface Schema extends Named, Described, Serializable, Extendable {

    Name ANONYMOUS_NAME = Name.of(Reserved.PREFIX + "anon");

    String VAR_THIS = "this";

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ObjectSchema.Builder.class)
    @JsonTypeIdResolver(TypeIdResolver.class)
    interface Descriptor<S extends Schema> extends Described, Extendable {

        String getType();

        Long getVersion();

        S build(Namespace namespace, Resolver.Constructing resolver, Version version, Name qualifiedName, int slot);

        default S build(final Name qualifiedName) {

            return build(null, Resolver.Constructing.ANONYMOUS, Version.CURRENT, qualifiedName, Schema.anonymousSlot());
        }

        default S build() {

            return build(Schema.anonymousQualifiedName());
        }

        interface Self<S extends Schema> extends Descriptor<S> {

            S self();

            @Override
            default Long getVersion() {

                return self().getVersion();
            }

            @Override
            default String getDescription() {

                return self().getDescription();
            }

            @Override
            default Map<String, Serializable> getExtensions() {

                return self().getExtensions();
            }
        }
    }

    interface Builder<B extends Builder<B, S>, S extends Schema> extends Descriptor<S>, Described.Builder<B>, Extendable.Builder<B> {

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

    int getSlot();

    long getVersion();

    static Name anonymousQualifiedName() {

        return ANONYMOUS_NAME.with(UUID.randomUUID().toString());
    }

    static int anonymousSlot() {

        return -1;
    }

    default boolean isAnonymous() {

        return getSlot() == anonymousSlot();
    }

    Descriptor<? extends Schema> descriptor();

    default Map<Name, Schema> dependencies() {

        return dependencies(Collections.emptySet());
    }

    default Map<Name, Schema> dependencies(final Set<Name> expand) {

        final Map<Name, Schema> dependencies = new HashMap<>();
        collectDependencies(expand, dependencies);
        return dependencies;
    }

    void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out);

    default Map<Name, Schema> materializationDependencies(final Set<Name> expand) {

        final Map<Name, Schema> dependencies = new HashMap<>();
        collectMaterializationDependencies(expand, dependencies);
        return dependencies;
    }

    void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, Schema> out);

    interface Resolver {

        interface Constructing extends Resolver {

            // 'Magic' method to handle cycles in namespace builder, instance under construction
            // must call resolver.constructing(this); as first constructor line.

            void constructing(final Name name, final Schema schema);

            // Convenience for anonymous construction

            Constructing ANONYMOUS = new Constructing() {

                @Override
                public void constructing(final Name name, final Schema schema) {

                }

                @Nullable
                @Override
                public Schema getSchema(final Name qualifiedName) {

                    return null;
                }
            };
        }

        Resolver NOOP = new Resolver() {

            @Nullable
            @Override
            public Schema getSchema(final Name qualifiedName) {

                return null;
            }
        };

        @Nullable
        Schema getSchema(Name qualifiedName);

        @Nonnull
        default Schema requireSchema(final Name qualifiedName) {

            final Schema result = getSchema(qualifiedName);
            if (result == null) {
                throw new MissingSchemaException(qualifiedName);
            } else {
                return result;
            }
        }

        @Nonnull
        default <T extends Schema> T requireSchema(final Name qualifiedName, final Class<T> cls) {

            final Schema schema = requireSchema(qualifiedName);
            if (cls.isAssignableFrom(schema.getClass())) {
                return cls.cast(schema);
            } else {
                throw new IllegalStateException(qualifiedName + " is not of type " + Schema.schemaTypeName(cls));
            }
        }

        @Nonnull
        default Schema requireSchema(final String name) {

            return requireSchema(Name.parse(name));
        }

        @Nonnull
        default <T extends Schema> T requireSchema(final String name, final Class<T> cls) {

            return requireSchema(Name.parse(name), cls);
        }

        @Nonnull
        default InstanceSchema requireInstanceSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, InstanceSchema.class);
        }

        default InstanceSchema requireInstanceSchema(final String name) {

            return requireInstanceSchema(Name.parse(name));
        }

        @Nonnull
        default ObjectSchema requireObjectSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, ObjectSchema.class);
        }

        default ObjectSchema requireObjectSchema(final String name) {

            return requireObjectSchema(Name.parse(name));
        }

        @Nonnull
        default StructSchema requireStructSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, StructSchema.class);
        }

        default StructSchema requireStructSchema(final String name) {

            return requireStructSchema(Name.parse(name));
        }

        @Nonnull
        default EnumSchema requireEnumSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, EnumSchema.class);
        }

        default EnumSchema requireEnumSchema(final String name) {

            return requireEnumSchema(Name.parse(name));
        }

        @Nonnull
        default ViewSchema requireViewSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, ViewSchema.class);
        }

        default ViewSchema requireViewSchema(final String name) {

            return requireViewSchema(Name.parse(name));
        }

        @Nonnull
        default QuerySchema requireQuerySchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, QuerySchema.class);
        }

        default QuerySchema requireQuerySchema(final String name) {

            return requireQuerySchema(Name.parse(name));
        }

        @Nonnull
        default LinkableSchema requireLinkableSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, LinkableSchema.class);
        }

        default LinkableSchema requireLinkableSchema(final String name) {

            return requireLinkableSchema(Name.parse(name));
        }

        @Nonnull
        default QueryableSchema requireQueryableSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, QueryableSchema.class);
        }

        default QueryableSchema requireQueryableSchema(final String name) {

            return requireQueryableSchema(Name.parse(name));
        }

        @Nonnull
        default InterfaceSchema requireInterfaceSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, InterfaceSchema.class);
        }

        default InterfaceSchema requireInterfaceSchema(final String name) {

            return requireInterfaceSchema(Name.parse(name));
        }

        @Nonnull
        default ReferableSchema requireReferableSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, ReferableSchema.class);
        }

        default ReferableSchema requireReferableSchema(final String name) {

            return requireReferableSchema(Name.parse(name));
        }

        @Nonnull
        default ValueSchema<?> requireValueSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, ValueSchema.class);
        }

        default ValueSchema<?> requireValueSchema(final String name) {

            return requireValueSchema(Name.parse(name));
        }

        @Nonnull
        default SequenceSchema requireSequenceSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, SequenceSchema.class);
        }

        default SequenceSchema requireSequenceSchema(final String name) {

            return requireSequenceSchema(Name.parse(name));
        }

        @Nonnull
        default CallableSchema requireCallableSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, CallableSchema.class);
        }

        default CallableSchema requireCallableSchema(final String name) {

            return requireCallableSchema(Name.parse(name));
        }

        @Nonnull
        default FunctionSchema requireFunctionSchema(final Name qualifiedName) {

            return requireSchema(qualifiedName, FunctionSchema.class);
        }

        default FunctionSchema requireFunctionSchema(final String name) {

            return requireFunctionSchema(Name.parse(name));
        }
    }

    static <T extends Schema> String schemaTypeName(final Class<T> cls) {

        final String stripSuffix = "Schema";
        final String longName = cls.getSimpleName();
        if (longName.endsWith(stripSuffix)) {
            return Text.lowerHyphen(cls.getSimpleName().substring(0, longName.length() - stripSuffix.length()));
        } else {
            return Text.lowerHyphen(longName);
        }
    }

    class TypeIdResolver extends TypeIdResolverBase {

        private static final SchemaClasspath CLASSPATH = SchemaClasspath.DEFAULT;

        private JavaType baseType;

        @Override
        public void init(final JavaType javaType) {

            this.baseType = javaType;
        }

        @Override
        public String idFromValue(final Object value) {

            return idFromValueAndType(value, value.getClass());
        }

        @Override
        @SuppressWarnings("unchecked")
        public String idFromValueAndType(final Object value, final Class<?> type) {

            return CLASSPATH.idForClass((Class<Schema.Builder<?, ?>>) type);
        }

        @Override
        public JavaType typeFromId(final DatabindContext databindContext, final String id) {

            final Class<?> cls;
            if (id == null) {
                cls = CLASSPATH.classForId(ObjectSchema.Builder.TYPE);
            } else {
                cls = CLASSPATH.classForId(id);
            }
            return databindContext.constructSpecializedType(baseType, cls);
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {

            return JsonTypeInfo.Id.NAME;
        }
    }
}
