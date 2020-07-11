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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.jackson.BasestarFactory;
import io.basestar.jackson.BasestarModule;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

//import io.basestar.util.Key;

/**
 * Namespace
 *
 * Container for schema objects
 *
 * <strong>Example</strong>
 * <pre>
 * MyObject:
 *   type: object
 * MyStruct:
 *   type: struct
 * </pre>
 */

@Getter
@EqualsAndHashCode
public class Namespace implements Serializable, Schema.Resolver {

   private static final ObjectMapper objectMapper = new ObjectMapper(new BasestarFactory(new YAMLFactory()
           .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false)
           .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
           .configure(YAMLGenerator.Feature.SPLIT_LINES, false)))
           .registerModule(new BasestarModule())
           .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    private final SortedMap<Name, Schema<?>> schemas;

    @Data
    @Accessors(chain = true)
    public static class Builder {

        private Map<String, Schema.Builder<?>> schemas;

        @JsonAnySetter
        public Builder setSchema(final String name, final Schema.Builder<?> schema) {

            schemas = Nullsafe.immutableCopyPut(schemas, name, schema);
            return this;
        }

        @JsonValue
        public Map<String, Schema.Builder<?>> getSchemas() {

            return schemas;
        }

        public Namespace build() {

            return new Namespace(this);
        }

        public void yaml(final OutputStream os) throws IOException {

            objectMapper.writeValue(os, this);
        }

        public void yaml(final Writer os) throws IOException {

            objectMapper.writeValue(os, this);
        }

        public static JsonSchema jsonSchema() throws IOException {

            final JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(objectMapper);
            return schemaGen.generateSchema(Property.Builder.class);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    public Namespace(final Builder builder) {

        final SortedMap<Name, Schema.Builder<?>> builders = ImmutableSortedMap.copyOf(builder.getSchemas().entrySet().stream().collect(Collectors.toMap(
                e -> Name.parse(e.getKey()),
                Map.Entry::getValue
        )));
        final Map<Name, Schema<?>> schemas = new HashMap<>();
        for(final Map.Entry<Name, Schema.Builder<?>> entry : builders.entrySet()) {
            resolveCyclic(entry.getKey(), entry.getValue(), builders, schemas);
        }
        this.schemas = ImmutableSortedMap.copyOf(schemas);
    }

    public Builder toBuilder() {

        return builder()
                .setSchemas(schemas.entrySet().stream().collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toBuilder()
                )));
    }

    private static Schema<?> resolveCyclic(final Name name, final Schema.Builder<?> builder,
                                           final SortedMap<Name, Schema.Builder<?>> builders,
                                           final Map<Name, Schema<?>> out) {

        if(out.containsKey(name)) {
            return out.get(name);
        } else {
            final int slot = builders.headMap(name).size();
            return builder.build(new Schema.Resolver.Constructing() {
               @Override
               public void constructing(final Schema<?> schema) {

                   assert !out.containsKey(name);
                   out.put(name, schema);
               }

               @Nullable
               @Override
               public Schema<?> getSchema(final Name name) {

                   final Schema.Builder<?> builder = builders.get(name);
                   if(builder == null) {
                       return null;
                   } else {
                       return resolveCyclic(name, builder, builders, out);
                   }
               }
            }, name, slot);
        }
    }

    @Override
    public Schema<?> getSchema(final Name qualifiedName) {

        return schemas.get(qualifiedName);
    }

//    public Map<String, Object> openApiSchemas() {
//
//        final Map<String, Object> result = new HashMap<>();
//        schemas.forEach((k, v) -> result.put(k, v.openApiSchema()));
//        return result;
//    }

    public static Namespace load(final URL... urls) throws IOException {

        final Map<String, Schema.Builder<?>> builders = new HashMap<>();
        for(final URL url : urls) {
            final Map<String, Schema.Builder<?>> schemas = objectMapper.readValue(url, new TypeReference<Map<String, Schema.Builder<?>>>(){});
            builders.putAll(schemas);
        }
        return new Builder()
                .setSchemas(builders)
                .build();
    }

    public static Namespace load(final InputStream... iss) throws IOException {

        final Map<String, Schema.Builder<?>> builders = new HashMap<>();
        for(final InputStream is : iss) {
            final Map<String, Schema.Builder<?>> schemas = objectMapper.readValue(is, new TypeReference<Map<String, Schema.Builder<?>>>(){});
            builders.putAll(schemas);
        }
        return new Builder()
                .setSchemas(builders)
                .build();
    }

    public void serialize(final OutputStream os) throws IOException {

        try(final ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(this);
        }
    }

    public static Namespace deserialize(final InputStream is) throws IOException, ClassNotFoundException {

        try(final ObjectInputStream ois = new ObjectInputStream(is)) {
            return (Namespace)ois.readObject();
        }
    }

    public void forEachEnumSchema(final BiConsumer<? super Name, ? super EnumSchema> fn) {

        schemas.forEach((k, v) -> {
            if(v instanceof EnumSchema) {
                fn.accept(k, (EnumSchema)v);
            }
        });
    }

    public void forEachInstanceSchema(final BiConsumer<? super Name, ? super InstanceSchema> fn) {

        schemas.forEach((k, v) -> {
            if(v instanceof InstanceSchema) {
                fn.accept(k, (InstanceSchema)v);
            }
        });
    }

    public void forEachStructSchema(final BiConsumer<? super Name, ? super StructSchema> fn) {

        schemas.forEach((k, v) -> {
            if(v instanceof StructSchema) {
                fn.accept(k, (StructSchema)v);
            }
        });
    }

    public void forEachObjectSchema(final BiConsumer<? super Name, ? super ObjectSchema> fn) {

        schemas.forEach((k, v) -> {
            if(v instanceof ObjectSchema) {
                fn.accept(k, (ObjectSchema)v);
            }
        });
    }

//    public ObjectSchema schemaOf(final Key key) {
//
//        final ObjectSchema root = requireRootSchema(key.first());
//        return root.schemaAt(key.withoutFirst());
//    }
//
//    public static boolean isContainer(final Key key) {
//
//        return key.size() % 2 == 1;
////    }
//
//    @Nonnull
//    @Override
//    public Set<Entry<String, Schema<?, ?>>> entrySet() {
//
//        return schemas.entrySet();
//    }
//
//    @Override
//    public Schema<?, ?> put(final String key, final Schema<?, ?>schema) {
//
//        return schemas.put(key, schema);
//    }
//
//    public Bound bind() {
//
//        return new Bound(this);
//    }
//
//    public ObjectSchema.Bound bindObjectSchema(final String name) {
//
//        return requireObjectSchema(name).bind(name, this);
//    }
//
//    @Data
//    class Bound {
//
//        private final Namespace namespace;
//
//        private final Map<String, Schema.Bound<?, ?>> properties;
//
//        public Bound(final Namespace namespace) {
//
//            this.namespace = namespace;
//            this.properties = namespace.getSchemas().entrySet().stream()
//                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().bind(e.getKey(), namespace)));
//        }
//    }
}
