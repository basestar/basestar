package io.basestar.graphql.schema;

/*-
 * #%L
 * basestar-graphql
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

import com.google.common.collect.ImmutableSet;
import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class SchemaConverter {

    private static final Set<String> SKIP_TYPE_NAMES = ImmutableSet.of("Query", "Mutation", "Subscription");

    public Namespace.Builder namespace(final TypeDefinitionRegistry tdr) {

        final Namespace.Builder builder = Namespace.builder();
        tdr.types().forEach((name, def) -> {
            if(!SKIP_TYPE_NAMES.contains(name)) {
                final Schema.Builder<?> schema = schema(def);
                if (schema != null) {
                    builder.setSchema(name, schema);
                }
            }
        });
        return builder;
    }

    private Schema.Builder<?> schema(final Node<?> def) {

        if(def instanceof ObjectTypeDefinition) {
            return instanceSchema((ObjectTypeDefinition) def);
        } else if(def instanceof InterfaceTypeDefinition) {
            return interfaceSchema((InterfaceTypeDefinition) def);
        } else if(def instanceof EnumTypeDefinition) {
            return enumSchema((EnumTypeDefinition)def);
        } else {
            log.info("Skipping definition of type {}", def.getClass().getName());
            return null;
        }
    }

    private EnumSchema.Builder enumSchema(final EnumTypeDefinition def) {

        final List<String> values = new ArrayList<>();
        for(final EnumValueDefinition valueDef : def.getEnumValueDefinitions()) {
            values.add(valueDef.getName());
        }
        return EnumSchema.builder().setValues(values);
    }

    private InstanceSchema.Builder instanceSchema(final ObjectTypeDefinition def) {

        // Heuristic
        if(def.getFieldDefinitions().stream().anyMatch(fieldDef -> Reserved.ID.equals(fieldDef.getName()))) {
            return objectSchema(def);
        } else {
            return structSchema(def);
        }
    }

    private InstanceSchema.Builder interfaceSchema(final InterfaceTypeDefinition def) {

        // Heuristic
        if(def.getFieldDefinitions().stream().anyMatch(fieldDef -> Reserved.ID.equals(fieldDef.getName()))) {
            final StructSchema.Builder builder = StructSchema.builder();
            builder.setProperties(instanceProperties(def.getFieldDefinitions(), Collections.emptySet()));
            return builder;
        } else {
            final ObjectSchema.Builder builder = ObjectSchema.builder();
            builder.setProperties(instanceProperties(def.getFieldDefinitions(), ObjectSchema.METADATA_SCHEMA.keySet()));
            return builder;
        }
    }

    private StructSchema.Builder structSchema(final ObjectTypeDefinition def) {

        final StructSchema.Builder builder = StructSchema.builder();
        def.getImplements().forEach(impl -> {
            // FIXME:
            builder.setExtend(Name.of(def.getName()));
        });
        builder.setProperties(instanceProperties(def.getFieldDefinitions(), Collections.emptySet()));
        return builder;
    }

    private ObjectSchema.Builder objectSchema(final ObjectTypeDefinition def) {

        final ObjectSchema.Builder builder = ObjectSchema.builder();
        def.getImplements().forEach(impl -> {
            // FIXME:
            builder.setExtend(Name.of(def.getName()));
        });
        builder.setProperties(instanceProperties(def.getFieldDefinitions(), ObjectSchema.METADATA_SCHEMA.keySet()));
        return builder;
    }

    private Map<String, Property.Builder> instanceProperties(final List<FieldDefinition> defs, final Set<String> skipMetadata) {

        final Map<String, Property.Builder> builder = new HashMap<>();
        defs.forEach(fieldDef -> {
            final String name = fieldDef.getName();
            if(!skipMetadata.contains(name)) {
                builder.put(name, Property.builder().setType(type(fieldDef.getType())));
            }
        });
        return builder;
    }

    private Use<?> type(final Type<?> type) {

        if(type instanceof NonNullType) {
            return type(((NonNullType)type).getType());
        } else if(type instanceof ListType) {
            return new UseArray<>(type(((ListType)type).getType()));
        } else if(type instanceof TypeName) {
            final String name = ((TypeName)type).getName();
            return type(name);
        } else {
            throw new IllegalStateException("Type " + type.getClass().getName() + " not understood");
        }
    }

    private Use<?> type(final String name) {

        switch (name) {
            case GraphQLUtils.ID_TYPE:
            case GraphQLUtils.STRING_TYPE:
                return UseString.DEFAULT;
            case GraphQLUtils.BOOLEAN_TYPE:
                return UseBoolean.DEFAULT;
            case GraphQLUtils.INT_TYPE:
                return UseInteger.DEFAULT;
            case GraphQLUtils.FLOAT_TYPE:
                return UseNumber.DEFAULT;
            default:
                return UseNamed.from(name);
        }
    }
}
