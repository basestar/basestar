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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Renaming;
import io.basestar.jackson.BasestarFactory;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.URLs;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URL;
import java.util.*;
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

    public interface Descriptor {

        Map<Name, Schema.Descriptor<?, ?>> getSchemas();

        @JsonValue
        default Map<String, Schema.Descriptor<?, ?>> jsonValue() {

            return getSchemas().entrySet().stream().collect(Collectors.toMap(
                    e -> e.getKey().toString(),
                    Map.Entry::getValue
            ));
        }

        default void yaml(final OutputStream os) throws IOException {

            YAML_MAPPER.writeValue(os, jsonValue());
        }

        default void yaml(final Writer os) throws IOException {

            YAML_MAPPER.writeValue(os, jsonValue());
        }

        default void json(final OutputStream os) throws IOException {

            JSON_MAPPER.writeValue(os, jsonValue());
        }

        default void json(final Writer os) throws IOException {

            JSON_MAPPER.writeValue(os, jsonValue());
        }
    }

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper(new BasestarFactory())
            .registerModule(new BasestarModule())
            // Do not use, Spark compatibility issue
            //.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new BasestarFactory(new YAMLFactory()
            .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false)
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
            .configure(YAMLGenerator.Feature.SPLIT_LINES, false)))
            .registerModule(new BasestarModule())
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
            // Do not use, Spark compatibility issue
            //.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    private final Version version;

    private final SortedMap<Name, Schema<?>> schemas;

    @Data
    @Accessors(chain = true)
    public static class Builder implements Descriptor {

        @JsonProperty("$version")
        private Version version;

        private Map<Name, Schema.Descriptor<?, ?>> schemas;

        public Builder setSchema(final Name name, final Schema.Descriptor<?, ?> schema) {

            schemas = Immutable.copyPut(schemas, name, schema);
            return this;
        }

        @JsonAnySetter
        public Builder setSchema(final String name, final Schema.Descriptor<?, ?> schema) {

            return setSchema(Name.parseNonEmpty(name), schema);
        }

        public Namespace build() {

            return build(Schema.Resolver.NOOP, Renaming.noop());
        }

        public Namespace build(final Name prefix) {

            return build(Schema.Resolver.NOOP, Renaming.addPrefix(prefix));
        }

        public Namespace build(final Schema.Resolver resolver) {

            return build(resolver, Renaming.noop());
        }

        public Namespace build(final Schema.Resolver resolver, final Renaming renaming) {

            return new Namespace(this, Nullsafe.orDefault(version, Version.CURRENT), resolver, renaming);
        }

        public static Builder load(final URL... urls) throws IOException {

            final Map<Name, Schema.Descriptor<?, ?>> builders = new HashMap<>();
            Version version = null;
            for(final URL url : URLs.all(urls)) {
                final Builder schemas = YAML_MAPPER.readValue(url, Builder.class);
                builders.putAll(schemas.getSchemas());
                version = version == null ? schemas.getVersion() : version;
            }
            return new Builder()
                    .setVersion(version)
                    .setSchemas(builders);
        }

        public static Builder load(final InputStream... iss) throws IOException {

            final Map<Name, Schema.Descriptor<?, ?>> builders = new HashMap<>();
            Version version = null;
            for(final InputStream is : iss) {
                final Builder schemas = YAML_MAPPER.readValue(is, Builder.class);
                builders.putAll(schemas.getSchemas());
                version = version == null ? schemas.getVersion() : version;
            }
            return new Builder()
                    .setVersion(version)
                    .setSchemas(builders);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Namespace(final Builder builder, final Version version, final Schema.Resolver resolver, final Renaming renaming) {

        this(builder.getSchemas(), version, resolver, renaming);
    }

    private Namespace(final Map<Name, Schema.Descriptor<?, ?>> schemas, final Version version, final Schema.Resolver resolver, final Renaming renaming) {

        this.version = version;
        final NavigableMap<Name, Schema.Descriptor<?, ?>> descriptors = ImmutableSortedMap.copyOf(schemas);
        final Set<Name> seen = new HashSet<>();
        descriptors.keySet().forEach(name -> {
            final Name rename = renaming.apply(name);
            if(!seen.add(rename)) {
                throw new SchemaValidationException(rename, "Cannot apply renaming, it will duplicate the name: " + rename);
            }
        });
        final Map<Name, Schema<?>> out = new HashMap<>();
        for(final Map.Entry<Name, Schema.Descriptor<?, ?>> entry : descriptors.entrySet()) {
            resolveCyclic(resolver, version, entry.getKey(), entry.getValue(), descriptors, renaming, out);
        }
        this.schemas = ImmutableSortedMap.copyOf(out);
    }

    private Namespace(final Map<Name, Schema<?>> schemas, final Version version) {

        this.schemas = ImmutableSortedMap.copyOf(schemas);
        this.version = version;
    }

    private static Schema<?> resolveCyclic(final Schema.Resolver resolver, final Version version, final Name inputName,
                                           final Schema.Descriptor<?, ?> descriptor, final NavigableMap<Name, Schema.Descriptor<?, ?>> descriptors,
                                           final Renaming naming, final Map<Name, Schema<?>> out) {

        final Name outputName = naming.apply(inputName);
        if(out.containsKey(outputName)) {
            return out.get(outputName);
        } else {
            final int slot = descriptors.headMap(inputName).size();
            return descriptor.build(new Schema.Resolver.Constructing() {
                @Override
                public void constructing(final Schema<?> schema) {

                    assert !out.containsKey(outputName);
                    out.put(outputName, schema);
                }

                @Nullable
                @Override
                public Schema<?> getSchema(final Name qualifiedName) {

                    final Schema.Descriptor<?, ?> builder = descriptors.get(qualifiedName);
                    if (builder == null) {
                        return resolver.getSchema(qualifiedName);
                    } else {
                        return resolveCyclic(resolver, version, qualifiedName, builder, descriptors, naming, out);
                    }
                }

                @Override
                public Collection<Schema<?>> getExtendedSchemas(final Name qualifiedName) {

                    return descriptors.entrySet().stream().filter(e -> {
                        final Schema.Descriptor<?, ?> descriptor = e.getValue();
                        final List<Name> extend;
                        if(descriptor instanceof ReferableSchema.Descriptor) {
                            extend = Nullsafe.orDefault(((ReferableSchema.Descriptor<?>) descriptor).getExtend());
                        } else {
                            extend = Collections.emptyList();
                        }
                        return extend.contains(qualifiedName);
                    }).map(e -> getSchema(e.getKey()))
                            .collect(Collectors.toList());
                }
            }, version, outputName, slot);
        }
    }

    @Override
    public Schema<?> getSchema(final Name qualifiedName) {

        return schemas.get(qualifiedName);
    }

    @Override
    public Collection<Schema<?>> getExtendedSchemas(final Name qualifiedName) {

        return schemas.values().stream().filter(schema -> {
            final List<Name> extend;
            if (schema instanceof ReferableSchema) {
                extend = Immutable.transform(((ReferableSchema) schema).getExtend(), Named::getQualifiedName);
            } else {
                extend = Collections.emptyList();
            }
            return extend.contains(qualifiedName);
        }).collect(Collectors.toList());
    }

    public static Namespace from(final Map<Name, Schema<?>> schemas) {

        return from(schemas, Version.CURRENT);
    }

    public static Namespace from(final Map<Name, Schema<?>> schemas, final Version version) {

        return new Namespace(schemas, version);
    }

    public Namespace relative(final Name root) {

        return rename(Renaming.removeOptionalPrefix(root));
    }

    public Namespace rename(final Renaming renaming) {

        return new Namespace(getSchemas().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().descriptor()
        )), version, Schema.Resolver.NOOP, renaming);
    }

    public static Namespace load(final Schema.Resolver resolver, final URL... urls) throws IOException {

        return Builder.load(urls).build(resolver);
    }

    public static Namespace load(final Schema.Resolver resolver, final InputStream... iss) throws IOException {

        return Builder.load(iss).build(resolver);
    }

    public static Namespace load(final URL... urls) throws IOException {

        return Builder.load(urls).build();
    }

    public static Namespace load(final InputStream... iss) throws IOException {

        return Builder.load(iss).build();
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

    public void forEachLinkableSchema(final BiConsumer<? super Name, ? super LinkableSchema> fn) {

        schemas.forEach((k, v) -> {
            if(v instanceof LinkableSchema) {
                fn.accept(k, (LinkableSchema)v);
            }
        });
    }

    public Descriptor descriptor() {

        return () -> schemas.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().descriptor()));
    }
}
