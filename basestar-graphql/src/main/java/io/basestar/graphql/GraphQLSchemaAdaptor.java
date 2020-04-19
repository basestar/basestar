package io.basestar.graphql;

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

import com.google.common.collect.ImmutableList;
import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.schema.*;
import io.basestar.schema.use.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphQLSchemaAdaptor {

    public static final String INPUT_PREFIX = "Input";

    public static final String INPUT_EXPR_PREFIX = "InputExpr";

    public static final String ENTRY_PREFIX = "Entry";

    public static final String ARRAY_PREFIX = "Array";

    public static final String ID_TYPE = "ID";

    public static final String STRING_TYPE = "String";

    public static final String INT_TYPE = "Int";

    public static final String FLOAT_TYPE = "Float";

    public static final String BOOLEAN_TYPE = "Boolean";

    public static final String INPUT_REF_TYPE = "InputRef";

    private final Namespace namespace;

    public GraphQLSchemaAdaptor(final Namespace namespace) {

        this.namespace = namespace;
    }

    public TypeDefinitionRegistry typeDefinitionRegistry() {

        final TypeDefinitionRegistry registry = new TypeDefinitionRegistry();
        final Map<String, Use<?>> mapTypes = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            registry.add(typeDefinition(schema));
            if(schema instanceof InstanceSchema) {
                final InstanceSchema instanceSchema = (InstanceSchema)schema;
                mapTypes.putAll(mapTypes(instanceSchema));
                registry.add(inputTypeDefinition(instanceSchema));
                if(schema instanceof ObjectSchema) {
                    registry.add(inputExpressionTypeDefinition(instanceSchema));
                }
            }
        });
        registry.add(queryDefinition());
        registry.add(mutationDefinition());
        registry.add(InputObjectTypeDefinition.newInputObjectDefinition()
                .name(INPUT_REF_TYPE)
                .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(Reserved.ID).type(new NonNullType(new TypeName(ID_TYPE))).build())
                .build());
        mapTypes.forEach((k, v) -> {
            registry.add(mapTypeDefinition(k, v));
            registry.add(inputMapTypeDefinition(k, v));
        });
//        registry.add(batchActionDefinition());
        return registry;
    }

    private ObjectTypeDefinition queryDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name("Query");
        namespace.getSchemas().forEach((schemaName, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                builder.fieldDefinition(readDefinition(objectSchema));
                builder.fieldDefinition(queryDefinition(objectSchema));
                objectSchema.getAllLinks()
                        .forEach((linkName, link) -> builder.fieldDefinition(queryLinkDefinition(objectSchema, link)));
            }
        });
        return builder.build();
    }

    public FieldDefinition readDefinition(final Schema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("read" + schema.getName());
        builder.type(new TypeName(schema.getName()));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.ID).type(new NonNullType(new TypeName(ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.VERSION).type(new TypeName(STRING_TYPE)).build());
        return builder.build();
    }

    public FieldDefinition queryDefinition(final Schema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("query" + schema.getName());
        builder.type(new ListType(new TypeName(schema.getName())));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name("query").type(new TypeName(STRING_TYPE)).build());
        return builder.build();
    }

    public FieldDefinition queryLinkDefinition(final Schema schema, final Link link) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("query" + schema.getName() + GraphQLUtils.ucFirst(link.getName()));
        builder.type(new ListType(new TypeName(link.getSchema().getName())));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.ID).type(new NonNullType(new TypeName(ID_TYPE))).build());
        return builder.build();
    }

    private ObjectTypeDefinition mutationDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name("Mutation");
        namespace.getSchemas().forEach((k, v) -> {
            if(v instanceof ObjectSchema) {
                builder.fieldDefinition(createDefinition(v));
                builder.fieldDefinition(updateDefinition(v));
                builder.fieldDefinition(deleteDefinition(v));
            }
        });
        return builder.build();
    }

    public FieldDefinition createDefinition(final Schema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("create" + schema.getName());
        builder.type(new TypeName(schema.getName()));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.ID).type(new TypeName(ID_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name("data").type(new TypeName(INPUT_PREFIX + schema.getName())).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name("expressions").type(new TypeName(INPUT_EXPR_PREFIX + schema.getName())).build());
        return builder.build();
    }

    public FieldDefinition updateDefinition(final Schema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("update" + schema.getName());
        builder.type(new TypeName(schema.getName()));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.ID).type(new NonNullType(new TypeName(ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.VERSION).type(new TypeName(INT_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name("data").type(new TypeName(INPUT_PREFIX + schema.getName())).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name("expressions").type(new TypeName(INPUT_EXPR_PREFIX + schema.getName())).build());
        return builder.build();
    }

    public FieldDefinition deleteDefinition(final Schema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name("delete" + schema.getName());
        builder.type(new TypeName(schema.getName()));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.ID).type(new NonNullType(new TypeName(ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(Reserved.VERSION).type(new TypeName(INT_TYPE)).build());
        return builder.build();
    }

    public InputObjectTypeDefinition inputTypeDefinition(final InstanceSchema schema) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(INPUT_PREFIX + schema.getName());
        builder.description(description(schema.getDescription()));
        schema.getAllProperties()
                .forEach((k, v) -> builder.inputValueDefinition(inputValueDefinition(v)));
        return builder.build();
    }


    private SDLDefinition inputExpressionTypeDefinition(final InstanceSchema schema) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(INPUT_EXPR_PREFIX + schema.getName());
        builder.description(description(schema.getDescription()));
        schema.getAllProperties()
                .forEach((k, v) -> builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(k).type(new TypeName(STRING_TYPE))
                        .build()));
        return builder.build();
    }

    public InputValueDefinition inputValueDefinition(final Property property) {

        final InputValueDefinition.Builder builder = InputValueDefinition.newInputValueDefinition();
        builder.name(property.getName());
        if(property.getDescription() != null) {
            builder.description(new Description(property.getDescription(), null, true));
        }
        // Cannot use NonNullType because value may come from an expression
        final Type<?> type = inputType(property.getType());
        builder.type(type);
        return builder.build();
    }

    public TypeDefinition typeDefinition(final Schema schema) {

        if (schema instanceof InstanceSchema) {
            return typeDefinition((InstanceSchema) schema);
        } else if (schema instanceof EnumSchema) {
            return typeDefinition((EnumSchema) schema);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public TypeDefinition typeDefinition(final InstanceSchema schema) {

        if(schema.isConcrete()) {
            final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
            builder.name(schema.getName());
            builder.description(description(schema.getDescription()));
            if (schema.getExtend() != null) {
                builder.implementz(implementz(schema));
            }
            fieldDefinitions(schema).forEach(builder::fieldDefinition);
            return builder.build();
        } else {
            final InterfaceTypeDefinition.Builder builder = InterfaceTypeDefinition.newInterfaceTypeDefinition();
            builder.name(schema.getName());
            builder.description(description(schema.getDescription()));
            fieldDefinitions(schema).forEach(builder::definition);
            return builder.build();
        }
    }

    private List<Type> implementz(final InstanceSchema schema) {

        final InstanceSchema parent = schema.getExtend();
        if(parent != null) {
            return ImmutableList.<Type>builder()
                    .addAll(implementz(parent))
                    .add(new TypeName(parent.getName()))
                    .build();
        } else {
            return ImmutableList.of();
        }
    }

    private List<FieldDefinition> fieldDefinitions(final InstanceSchema schema) {

        final List<FieldDefinition> fields = new ArrayList<>();
        schema.metadataSchema()
                .forEach((k, v) -> fields.add(metadataFieldDefinition(k, v)));
        schema.getAllProperties()
                .forEach((k, v) -> fields.add(fieldDefinition(v)));
        if(schema instanceof Link.Resolver) {
            ((Link.Resolver) schema).getAllLinks()
                    .forEach((k, v) -> fields.add(FieldDefinition.newFieldDefinition()
                            .name(k)
                            .type(new ListType(new TypeName(v.getSchema().getName())))
                            .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                                    .name("query").type(new TypeName(STRING_TYPE)).build())
                            .build()));
        }
        if(schema instanceof Transient.Resolver) {
            ((Transient.Resolver) schema).getDeclaredTransients()
                    .forEach((k, v) -> {
                        // Can only show typed transients
                        if(v.getType() != null) {
                            fields.add(FieldDefinition.newFieldDefinition()
                                    .name(k)
                                    .type(type(v.getType()))
                                    .build());
                        }
                    });
        }
        return fields;
    }

    private Description description(final String description) {

        if(description != null) {
            return new Description(description, null, true);
        } else {
            return null;
        }
    }

    public EnumTypeDefinition typeDefinition(final EnumSchema schema) {

        final EnumTypeDefinition.Builder builder = EnumTypeDefinition.newEnumTypeDefinition();
        builder.name(schema.getName());
        if(schema.getDescription() != null) {
            builder.description(new Description(schema.getDescription(), null, true));
        }
        schema.getValues().forEach(v -> builder.enumValueDefinition(EnumValueDefinition.newEnumValueDefinition()
                .name(v).build()));
        return builder.build();
    }

    public FieldDefinition fieldDefinition(final Property property) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(property.getName());
        if(property.getDescription() != null) {
            builder.description(new Description(property.getDescription(), null, true));
        }
        final Type<?> type = type(property.getType());
        builder.type(property.isRequired() ? new NonNullType(type) : type);
        return builder.build();
    }

    public FieldDefinition metadataFieldDefinition(final String name, final Use<?> type) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(name);
        if(Reserved.ID.equals(name)) {
            builder.type(new NonNullType(new TypeName(ID_TYPE)));
        } else {
            builder.type(new NonNullType(type(type)));
        }
        return builder.build();
    }

    private InputObjectTypeDefinition inputMapTypeDefinition(final String name, final Use<?> type) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(INPUT_PREFIX + name);
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(GraphQLUtils.MAP_KEY).type(new NonNullType(new TypeName(STRING_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(GraphQLUtils.MAP_VALUE).type(inputType(type)).build());
        return builder.build();
    }

    private ObjectTypeDefinition mapTypeDefinition(final String name, final Use<?> type) {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(name);
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(GraphQLUtils.MAP_KEY).type(new NonNullType(new TypeName(STRING_TYPE))).build());
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(GraphQLUtils.MAP_VALUE).type(type(type)).build());
        return builder.build();
    }

    public Type type(final Use<?> type) {

        return type.visit(new Use.Visitor<Type<?>>() {

            @Override
            public Type<?> visitBoolean(final UseBoolean type) {

                return new TypeName(BOOLEAN_TYPE);
            }

            @Override
            public Type<?> visitInteger(final UseInteger type) {

                return new TypeName(INT_TYPE);
            }

            @Override
            public Type<?> visitNumber(final UseNumber type) {

                return new TypeName(FLOAT_TYPE);
            }

            @Override
            public Type<?> visitString(final UseString type) {

                return new TypeName(STRING_TYPE);
            }

            @Override
            public Type<?> visitEnum(final UseEnum type) {

                return new TypeName(type.getType().getName());
            }

            @Override
            public Type<?> visitRef(final UseRef type) {

                return new TypeName(type.getSchema().getName());
            }

            @Override
            public <T> Type<?> visitArray(final UseArray<T> type) {

                return new ListType(type.getType().visit(this));
            }

            @Override
            public <T> Type<?> visitSet(final UseSet<T> type) {

                return new ListType(type.getType().visit(this));
            }

            @Override
            public <T> Type<?> visitMap(final UseMap<T> type) {

                return new ListType(new TypeName(mapEntryTypeName(type.getType())));
            }

            @Override
            public Type<?> visitStruct(final UseStruct type) {

                return new TypeName(type.getSchema().getName());
            }

            @Override
            public Type<?> visitBinary(final UseBinary type) {

                return new TypeName(STRING_TYPE);
            }
        });
    }

    public Type inputType(final Use<?> type) {

        return type.visit(new Use.Visitor<Type<?>>() {

            @Override
            public Type<?> visitBoolean(final UseBoolean type) {

                return new TypeName(BOOLEAN_TYPE);
            }

            @Override
            public Type<?> visitInteger(final UseInteger type) {

                return new TypeName(INT_TYPE);
            }

            @Override
            public Type<?> visitNumber(final UseNumber type) {

                return new TypeName(FLOAT_TYPE);
            }

            @Override
            public Type<?> visitString(final UseString type) {

                return new TypeName(STRING_TYPE);
            }

            @Override
            public Type<?> visitEnum(final UseEnum type) {

                return new TypeName(type.getType().getName());
            }

            @Override
            public Type<?> visitRef(final UseRef type) {

                return new TypeName(INPUT_REF_TYPE);
            }

            @Override
            public <T> Type<?> visitArray(final UseArray<T> type) {

                return new ListType(type.getType().visit(this));
            }

            @Override
            public <T> Type<?> visitSet(final UseSet<T> type) {

                return new ListType(type.getType().visit(this));
            }

            @Override
            public <T> Type<?> visitMap(final UseMap<T> type) {

                return new ListType(new TypeName(INPUT_PREFIX + mapEntryTypeName(type.getType())));
            }

            @Override
            public Type<?> visitStruct(final UseStruct type) {

                return new TypeName(INPUT_PREFIX + type.getSchema().getName());
            }

            @Override
            public Type<?> visitBinary(final UseBinary type) {

                return new TypeName(STRING_TYPE);
            }
        });
    }

    private String mapEntryTypeName(final Use<?> type) {

        return ENTRY_PREFIX + type.visit(new Use.Visitor<String>() {

            @Override
            public String visitBoolean(final UseBoolean type) {

                return BOOLEAN_TYPE;
            }

            @Override
            public String visitInteger(final UseInteger type) {

                return INT_TYPE;
            }

            @Override
            public String visitNumber(final UseNumber type) {

                return FLOAT_TYPE;
            }

            @Override
            public String visitString(final UseString type) {

                return STRING_TYPE;
            }

            @Override
            public String visitEnum(final UseEnum type) {

                return type.getType().getName();
            }

            @Override
            public String visitRef(final UseRef type) {

                return type.getSchema().getName();
            }

            @Override
            public <T> String visitArray(final UseArray<T> type) {

                return ARRAY_PREFIX + type.getType().visit(this);
            }

            @Override
            public <T> String visitSet(final UseSet<T> type) {

                return ARRAY_PREFIX + type.getType().visit(this);
            }

            @Override
            public <T> String visitMap(final UseMap<T> type) {

                return ENTRY_PREFIX + type.getType().visit(this);
            }

            @Override
            public String visitStruct(final UseStruct type) {

                return type.getSchema().getName();
            }

            @Override
            public String visitBinary(final UseBinary type) {

                return STRING_TYPE;
            }
        });
    }

    private Map<String, Use<?>> mapTypes(final InstanceSchema schema) {

        final Map<String, Use<?>> mapTypes = new HashMap<>();
        schema.getDeclaredProperties().forEach((k, v) -> {
            v.getType().visit(new Use.Visitor<Void>() {

                @Override
                public Void visitBoolean(final UseBoolean type) {

                    return null;
                }

                @Override
                public Void visitInteger(final UseInteger type) {

                    return null;
                }

                @Override
                public Void visitNumber(final UseNumber type) {

                    return null;
                }

                @Override
                public Void visitString(final UseString type) {

                    return null;
                }

                @Override
                public Void visitEnum(final UseEnum type) {

                    return null;
                }

                @Override
                public Void visitRef(final UseRef type) {

                    return null;
                }

                @Override
                public <T> Void visitArray(final UseArray<T> type) {

                    type.getType().visit(this);
                    return null;
                }

                @Override
                public <T> Void visitSet(final UseSet<T> type) {

                    type.getType().visit(this);
                    return null;
                }

                @Override
                public <T> Void visitMap(final UseMap<T> type) {

                    type.getType().visit(this);
                    mapTypes.put(mapEntryTypeName(type.getType()), type.getType());
                    return null;
                }

                @Override
                public Void visitStruct(final UseStruct type) {

                    return null;
                }

                @Override
                public Void visitBinary(final UseBinary type) {

                    return null;
                }
            });
        });
        return mapTypes;
    }

}
