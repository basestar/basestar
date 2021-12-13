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

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.basestar.graphql.GraphQLStrategy;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Immutable;
import io.basestar.util.Name;

import java.util.*;
import java.util.stream.Stream;

public class SchemaAdaptor {

    private final Namespace namespace;

    private final GraphQLStrategy strategy;

    public SchemaAdaptor(final Namespace namespace, final GraphQLStrategy strategy) {

        this.namespace = namespace;
        this.strategy = strategy;
    }

    public TypeDefinitionRegistry typeDefinitionRegistry() {

        final TypeDefinitionRegistry registry = new TypeDefinitionRegistry();
        Stream.of(strategy.anyTypeName(), strategy.dateTypeName(), strategy.dateTimeTypeName(),
                        strategy.secretTypeName(), strategy.binaryTypeName(), strategy.decimalTypeName())
                .forEach(scalar -> {
                    registry.add(new ScalarTypeDefinition(scalar));
                    if (namespace.getSchema(Name.parse(scalar)) != null) {
                        throw new IllegalStateException("Namespace defines a schema called '" + scalar + "' but this is a scalar type name");
                    }
                });
        final Map<String, Use<?>> mapTypes = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            typeDefinition(schema).ifPresent(registry::add);
            if (schema instanceof InstanceSchema) {
                final InstanceSchema instanceSchema = (InstanceSchema) schema;
                mapTypes.putAll(mapTypes(instanceSchema));
                if (schema instanceof ObjectSchema) {
                    final ObjectSchema objectSchema = (ObjectSchema) instanceSchema;
                    registry.add(pageTypeDefinition(objectSchema));
                    if (objectSchema.hasProperties()) {
                        registry.add(createInputTypeDefinition(objectSchema));
                        registry.add(inputExpressionTypeDefinition(objectSchema));
                    }
                    if (objectSchema.hasMutableProperties()) {
                        registry.add(updateInputTypeDefinition(objectSchema));
                        registry.add(patchInputTypeDefinition(objectSchema));
                    }
                } else if (schema instanceof InterfaceSchema) {
                    final InterfaceSchema interfaceSchema = (InterfaceSchema) instanceSchema;
                    registry.add(pageTypeDefinition(interfaceSchema));
                    registry.add(missingInterfaceRefDefinition(interfaceSchema));
                } else if (schema instanceof ViewSchema) {
                    final ViewSchema viewSchema = (ViewSchema) instanceSchema;
                    registry.add(pageTypeDefinition(viewSchema));
                } else {
                    registry.add(inputTypeDefinition(instanceSchema));
                }
            }
        });
        registry.add(queryDefinition());
        if (namespace.hasObjectSchemas()) {
            registry.add(mutationDefinition());
            registry.add(subscriptionDefinition());
            registry.add(batchTypeDefinition());
        }
        registry.add(InputObjectTypeDefinition.newInputObjectDefinition()
                .name(strategy.inputRefTypeName(false))
                .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build())
                .build());
        registry.add(InputObjectTypeDefinition.newInputObjectDefinition()
                .name(strategy.inputRefTypeName(true))
                .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build())
                .inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(strategy.versionArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.INT_TYPE))).build())
                .build());
        registry.add(consistencyTypeDefinition());
        mapTypes.forEach((k, v) -> {
            registry.add(mapEntryTypeDefinition(v));
            registry.add(inputMapEntryTypeDefinition(v));
        });
        return registry;
    }

    private ObjectTypeDefinition pageTypeDefinition(final InstanceSchema instanceSchema) {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(strategy.pageTypeName(instanceSchema));
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(strategy.pageItemsFieldName())
                .type(new ListType(new TypeName(strategy.typeName(instanceSchema))))
                .build());
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(strategy.pagePagingFieldName())
                .type(new TypeName(GraphQLUtils.STRING_TYPE))
                .build());
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(strategy.pageTotalFieldName())
                .type(new TypeName(GraphQLUtils.INT_TYPE))
                .build());
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(strategy.pageApproxTotalFieldName())
                .type(new TypeName(GraphQLUtils.INT_TYPE))
                .build());
        return builder.build();
    }

    private ObjectTypeDefinition queryDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(GraphQLUtils.QUERY_TYPE);
        namespace.forEachLinkableSchema((schemaName, schema) -> {
            if (schema instanceof ReferableSchema) {
                builder.fieldDefinition(readDefinition((ReferableSchema) schema));
            }
            builder.fieldDefinition(queryDefinition(schema));
            schema.getLinks()
                    .forEach((linkName, link) -> builder.fieldDefinition(queryLinkDefinition(schema, link)));
        });
        return builder.build();
    }

    public FieldDefinition readDefinition(final ReferableSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.readMethodName(schema));
        builder.type(new TypeName(strategy.typeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.versionArgumentName()).type(new TypeName(GraphQLUtils.INT_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName()).type(new TypeName(strategy.consistencyTypeName())).build());
        return builder.build();
    }

    public FieldDefinition queryDefinition(final LinkableSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.queryMethodName(schema));
        builder.type(new TypeName(strategy.pageTypeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.queryArgumentName()).type(new TypeName(GraphQLUtils.STRING_TYPE)).build());
        addQueryArguments(builder);
        return builder.build();
    }

    public FieldDefinition queryLinkDefinition(final LinkableSchema schema, final Link link) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.queryLinkMethodName(schema, link));
        builder.type(new TypeName(strategy.pageTypeName(link.getSchema())));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build());
        addQueryArguments(builder);
        return builder.build();
    }

    private void addQueryArguments(final FieldDefinition.Builder builder) {

        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.sortArgumentName()).type(new ListType(new TypeName(GraphQLUtils.STRING_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.countArgumentName()).type(new TypeName(GraphQLUtils.INT_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.pagingArgumentName()).type(new TypeName(GraphQLUtils.STRING_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName()).type(new TypeName(strategy.consistencyTypeName())).build());
    }

    private ObjectTypeDefinition mutationDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(GraphQLUtils.MUTATION_TYPE);
        addMutations(builder);
        builder.fieldDefinition(batchDefinition());
        return builder.build();
    }

    private void addMutations(final ObjectTypeDefinition.Builder builder) {

        namespace.forEachObjectSchema((k, v) -> {
            if (!v.isReadonly()) {
                builder.fieldDefinition(createDefinition(v));
                if (v.hasMutableProperties()) {
                    builder.fieldDefinition(updateDefinition(v));
                    builder.fieldDefinition(patchDefinition(v));
                }
                builder.fieldDefinition(deleteDefinition(v));
            }
        });
    }

    private FieldDefinition batchDefinition() {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.batchMethodName());
        builder.type(new TypeName(strategy.batchTypeName()));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName())
                .type(new TypeName(strategy.consistencyTypeName())).build());
        return builder.build();
    }

    private ObjectTypeDefinition batchTypeDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(strategy.batchTypeName());
        addMutations(builder);
        return builder.build();
    }

    private EnumTypeDefinition consistencyTypeDefinition() {

        final EnumTypeDefinition.Builder builder = EnumTypeDefinition.newEnumTypeDefinition();
        builder.name(strategy.consistencyTypeName());
        Arrays.stream(Consistency.values()).forEach(v -> {
            if (v != Consistency.NONE) {
                builder.enumValueDefinition(EnumValueDefinition.newEnumValueDefinition().name(v.name()).build());
            }
        });
        return builder.build();
    }

    public FieldDefinition createDefinition(final ObjectSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.createMethodName(schema));
        builder.type(new TypeName(strategy.typeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new TypeName(GraphQLUtils.ID_TYPE)).build());
        if (schema.hasProperties()) {
            builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                    .name(strategy.dataArgumentName()).type(new TypeName(strategy.createInputTypeName(schema))).build());
        }
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName()).type(new TypeName(strategy.consistencyTypeName())).build());
        if (schema.hasProperties()) {
            builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                    .name(strategy.expressionsArgumentName()).type(new TypeName(strategy.inputExpressionsTypeName(schema))).build());
        }
        return builder.build();
    }

    public FieldDefinition updateDefinition(final ObjectSchema schema, final String methodName, final String typeName) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(methodName);
        builder.type(new TypeName(strategy.typeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.versionArgumentName()).type(new TypeName(GraphQLUtils.INT_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.dataArgumentName()).type(new TypeName(typeName)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName()).type(new TypeName(strategy.consistencyTypeName())).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.expressionsArgumentName()).type(new TypeName(strategy.inputExpressionsTypeName(schema))).build());
        return builder.build();
    }

    public FieldDefinition updateDefinition(final ObjectSchema schema) {

        return updateDefinition(schema, strategy.updateMethodName(schema), strategy.updateInputTypeName(schema));
    }

    public FieldDefinition patchDefinition(final ObjectSchema schema) {

        return updateDefinition(schema, strategy.patchMethodName(schema), strategy.patchInputTypeName(schema));
    }

    public FieldDefinition deleteDefinition(final ObjectSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.deleteMethodName(schema));
        builder.type(new TypeName(strategy.typeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.consistencyArgumentName()).type(new TypeName(strategy.consistencyTypeName())).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.versionArgumentName()).type(new TypeName(GraphQLUtils.INT_TYPE)).build());
        return builder.build();
    }

    private ObjectTypeDefinition subscriptionDefinition() {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(GraphQLUtils.SUBSCRIPTION_TYPE);
        namespace.forEachObjectSchema((schemaName, schema) -> {
            builder.fieldDefinition(subscribeDefinition(schema));
            builder.fieldDefinition(subscribeQueryDefinition(schema));
        });
        return builder.build();
    }

    public FieldDefinition subscribeDefinition(final ObjectSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.subscribeMethodName(schema));
        builder.type(new TypeName(strategy.typeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.idArgumentName()).type(new NonNullType(new TypeName(GraphQLUtils.ID_TYPE))).build());
        return builder.build();
    }

    public FieldDefinition subscribeQueryDefinition(final ObjectSchema schema) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(strategy.subscribeQueryMethodName(schema));
        builder.type(new TypeName(strategy.pageTypeName(schema)));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.queryArgumentName()).type(new TypeName(GraphQLUtils.STRING_TYPE)).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.sortArgumentName()).type(new ListType(new TypeName(GraphQLUtils.STRING_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(strategy.countArgumentName()).type(new TypeName(GraphQLUtils.INT_TYPE)).build());
        return builder.build();
    }

    public InputObjectTypeDefinition inputTypeDefinition(final InstanceSchema schema) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(strategy.inputTypeName(schema));
        builder.description(description(schema.getDescription()));
        schema.getProperties()
                .forEach((k, v) -> builder.inputValueDefinition(inputValueDefinition(v, true)));
        return builder.build();
    }

    public InputObjectTypeDefinition inputTypeDefinition(final ObjectSchema schema, final String name, final boolean create, final boolean required) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(name);
        builder.description(description(schema.getDescription()));
        schema.getProperties()
                .forEach((k, v) -> {
                    if (create || !v.isImmutable()) {
                        builder.inputValueDefinition(inputValueDefinition(v, required));
                    }
                });
        return builder.build();
    }

    public InputObjectTypeDefinition createInputTypeDefinition(final ObjectSchema schema) {

        return inputTypeDefinition(schema, strategy.createInputTypeName(schema), true, true);
    }

    public InputObjectTypeDefinition updateInputTypeDefinition(final ObjectSchema schema) {

        return inputTypeDefinition(schema, strategy.updateInputTypeName(schema), false, true);
    }

    public InputObjectTypeDefinition patchInputTypeDefinition(final ObjectSchema schema) {

        return inputTypeDefinition(schema, strategy.patchInputTypeName(schema), true, false);
    }

    private SDLDefinition<?> inputExpressionTypeDefinition(final InstanceSchema schema) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(strategy.inputExpressionsTypeName(schema));
        builder.description(description(schema.getDescription()));
        schema.getProperties()
                .forEach((k, v) -> builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                        .name(k).type(new TypeName(GraphQLUtils.STRING_TYPE))
                        .build()));
        return builder.build();
    }

    public InputValueDefinition inputValueDefinition(final Property property, final boolean required) {

        final InputValueDefinition.Builder builder = InputValueDefinition.newInputValueDefinition();
        builder.name(property.getName());
        if (property.getDescription() != null) {
            builder.description(new Description(property.getDescription(), null, true));
        }
        final Type<?> type = inputType(required ? property.typeOf() : property.typeOf().optional(true));
        builder.type(type);
        return builder.build();
    }

    public TypeDefinition<?> missingInterfaceRefDefinition(final ReferableSchema schema) {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(strategy.missingInterfaceRefTypeName(schema));
        builder.description(description("Generated stub for missing interface references"));
        builder.implementz(new TypeName(strategy.typeName(schema)));
        fieldDefinitions(schema).forEach(builder::fieldDefinition);
        return builder.build();
    }

    public Optional<TypeDefinition<?>> typeDefinition(final Schema<?> schema) {

        if (schema instanceof InstanceSchema) {
            return Optional.of(typeDefinition((InstanceSchema) schema));
        } else if (schema instanceof EnumSchema) {
            return Optional.of(typeDefinition((EnumSchema) schema));
        } else {
            return Optional.empty();
        }
    }

    public TypeDefinition<?> typeDefinition(final InstanceSchema schema) {

        if (schema.isConcrete()) {
            final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
            builder.name(strategy.typeName(schema));
            builder.description(description(schema.getDescription()));
            if (schema instanceof ReferableSchema) {
                builder.implementz(implementz((ReferableSchema) schema));
            }
            fieldDefinitions(schema).forEach(builder::fieldDefinition);
            return builder.build();
        } else {
            final InterfaceTypeDefinition.Builder builder = InterfaceTypeDefinition.newInterfaceTypeDefinition();
            builder.name(strategy.typeName(schema));
            builder.description(description(schema.getDescription()));
            fieldDefinitions(schema).forEach(builder::definition);
            return builder.build();
        }
    }

    @SuppressWarnings("rawtypes")
    private List<Type> implementz(final ReferableSchema schema) {

        final List<? extends ReferableSchema> extend = schema.getExtend();
        if (extend != null) {
            final List<Type> result = new ArrayList<>();
            for (final ReferableSchema parent : extend) {
                result.addAll(implementz(parent));
                result.add(new TypeName(strategy.typeName(parent)));
            }
            return result;
        } else {
            return Immutable.list();
        }
    }

    private List<FieldDefinition> fieldDefinitions(final InstanceSchema schema) {

        final List<FieldDefinition> fields = new ArrayList<>();
        schema.metadataSchema()
                .forEach((k, v) -> {
                    if (!Reserved.isReserved(k)) {
                        fields.add(metadataFieldDefinition(k, v));
                    }
                });
        schema.getProperties()
                .forEach((k, v) -> {
                    if (!v.isAlwaysHidden()) {
                        fields.add(fieldDefinition(v));
                    }
                });
        if (schema instanceof Link.Resolver) {
            ((Link.Resolver) schema).getLinks()
                    .forEach((k, v) -> {
                        if (!v.isAlwaysHidden()) {
                            fields.add(FieldDefinition.newFieldDefinition()
                                    .name(k)
                                    .type(new TypeName(strategy.pageTypeName(v.getSchema())))
                                    .build());
                        }
                    });
        }
        if (schema instanceof Transient.Resolver) {
            ((Transient.Resolver) schema).getDeclaredTransients()
                    .forEach((k, v) -> {
                        // Can only show typed transients
                        if (!v.isAlwaysHidden() && v.typeOf() != null) {
                            fields.add(FieldDefinition.newFieldDefinition()
                                    .name(k)
                                    .type(type(v.typeOf()))
                                    .build());
                        }
                    });
        }
        return fields;
    }

    private Description description(final String description) {

        if (description != null) {
            return new Description(description, null, true);
        } else {
            return null;
        }
    }

    public EnumTypeDefinition typeDefinition(final EnumSchema schema) {

        final EnumTypeDefinition.Builder builder = EnumTypeDefinition.newEnumTypeDefinition();
        builder.name(strategy.typeName(schema));
        if (schema.getDescription() != null) {
            builder.description(new Description(schema.getDescription(), null, true));
        }
        schema.getValues().forEach(v -> builder.enumValueDefinition(EnumValueDefinition.newEnumValueDefinition()
                .name(v).build()));
        return builder.build();
    }

    public FieldDefinition fieldDefinition(final Property property) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(property.getName());
        if (property.getDescription() != null) {
            builder.description(new Description(property.getDescription(), null, true));
        }
        final Type<?> type = type(property.typeOf());
        builder.type(type);
        return builder.build();
    }

    public FieldDefinition metadataFieldDefinition(final String name, final Use<?> type) {

        final FieldDefinition.Builder builder = FieldDefinition.newFieldDefinition();
        builder.name(name);
        if (strategy.idArgumentName().equals(name)) {
            builder.type(new TypeName(GraphQLUtils.ID_TYPE));
        } else {
            builder.type(type(type));
        }
        return builder.build();
    }

    private InputObjectTypeDefinition inputMapEntryTypeDefinition(final Use<?> type) {

        final InputObjectTypeDefinition.Builder builder = InputObjectTypeDefinition.newInputObjectDefinition();
        builder.name(strategy.inputMapEntryTypeName(type));
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(GraphQLUtils.MAP_KEY).type(new NonNullType(new TypeName(GraphQLUtils.STRING_TYPE))).build());
        builder.inputValueDefinition(InputValueDefinition.newInputValueDefinition()
                .name(GraphQLUtils.MAP_VALUE).type(inputType(type)).build());
        return builder.build();
    }

    private ObjectTypeDefinition mapEntryTypeDefinition(final Use<?> type) {

        final ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();
        builder.name(strategy.mapEntryTypeName(type));
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(GraphQLUtils.MAP_KEY).type(new NonNullType(new TypeName(GraphQLUtils.STRING_TYPE))).build());
        builder.fieldDefinition(FieldDefinition.newFieldDefinition()
                .name(GraphQLUtils.MAP_VALUE).type(type(type)).build());
        return builder.build();
    }

    // FIXME: generalize with inputType
    // FIXME: review what happens when we respond null to a non-null type at GQL
    public Type<?> type(final Use<?> type) {

        return typeImpl(type);
    }

    private Type<?> typeImpl(final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Type<?>>() {

            @Override
            public Type<?> visitBoolean(final UseBoolean type) {

                return new TypeName(GraphQLUtils.BOOLEAN_TYPE);
            }

            @Override
            public Type<?> visitInteger(final UseInteger type) {

                return new TypeName(GraphQLUtils.INT_TYPE);
            }

            @Override
            public Type<?> visitNumber(final UseNumber type) {

                return new TypeName(GraphQLUtils.FLOAT_TYPE);
            }

            @Override
            public <T> Type<?> visitStringLike(final UseStringLike<T> type) {

                return new TypeName(GraphQLUtils.STRING_TYPE);
            }

            @Override
            public Type<?> visitDate(final UseDate type) {

                return new TypeName(strategy.dateTypeName());
            }

            @Override
            public Type<?> visitDateTime(final UseDateTime type) {

                return new TypeName(strategy.dateTimeTypeName());
            }

            @Override
            public Type<?> visitSecret(final UseSecret type) {

                return new TypeName(strategy.secretTypeName());
            }

            @Override
            public Type<?> visitEnum(final UseEnum type) {

                return new TypeName(strategy.typeName(type.getSchema()));
            }

            @Override
            public <V, T extends Collection<V>> Type<?> visitCollection(final UseCollection<V, T> type) {

                return new ListType(type(type.getType()));
            }

            @Override
            public <T> Type<?> visitPage(final UsePage<T> type) {

                final Use<T> itemType = type.getType();
                if (itemType instanceof UseLinkable) {
                    final LinkableSchema schema = ((UseLinkable) itemType).getSchema();
                    return new TypeName(strategy.pageTypeName(schema));
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public <T> Type<?> visitMap(final UseMap<T> type) {

                return new ListType(new TypeName(strategy.mapEntryTypeName(type.getType())));
            }

            @Override
            public Type<?> visitInstance(final UseInstance type) {

                return new TypeName(strategy.typeName(type.getSchema()));
            }

            @Override
            public Type<?> visitBinary(final UseBinary type) {

                return new TypeName(strategy.binaryTypeName());
            }

            @Override
            public Type<?> visitDecimal(final UseDecimal type) {

                return new TypeName(strategy.decimalTypeName());
            }

            @Override
            public Type<?> visitAny(final UseAny type) {

                return new TypeName(strategy.anyTypeName());
            }
        });
    }

    // FIXME: generalize with type
    public Type<?> inputType(final Use<?> type) {

        if (type.isOptional()) {
            return inputTypeImpl(type);
        } else {
            return new NonNullType(inputTypeImpl(type));
        }
    }

    public Type<?> inputTypeImpl(final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Type<?>>() {

            @Override
            public <T> Type<?> visitDefault(final Use<T> type) {

                return null;
            }

            @Override
            public Type<?> visitBoolean(final UseBoolean type) {

                return new TypeName(GraphQLUtils.BOOLEAN_TYPE);
            }

            @Override
            public Type<?> visitInteger(final UseInteger type) {

                return new TypeName(GraphQLUtils.INT_TYPE);
            }

            @Override
            public Type<?> visitNumber(final UseNumber type) {

                return new TypeName(GraphQLUtils.FLOAT_TYPE);
            }

            @Override
            public Type<?> visitString(final UseString type) {

                return new TypeName(GraphQLUtils.STRING_TYPE);
            }

            @Override
            public Type<?> visitSecret(final UseSecret type) {

                return new TypeName(strategy.secretTypeName());
            }

            @Override
            public Type<?> visitEnum(final UseEnum type) {

                return new TypeName(strategy.typeName(type.getSchema()));
            }

            @Override
            public Type<?> visitRef(final UseRef type) {

                return new TypeName(strategy.inputRefTypeName(type.isVersioned()));
            }

            @Override
            public <T> Type<?> visitArray(final UseArray<T> type) {

                return new ListType(inputType(type.getType()));
            }

            @Override
            public <T> Type<?> visitSet(final UseSet<T> type) {

                return new ListType(inputType(type.getType()));
            }

            @Override
            public <T> Type<?> visitMap(final UseMap<T> type) {

                return new ListType(new TypeName(strategy.inputMapEntryTypeName(type.getType())));
            }

            @Override
            public Type<?> visitStruct(final UseStruct type) {

                return new TypeName(strategy.inputTypeName(type.getSchema()));
            }

            @Override
            public Type<?> visitBinary(final UseBinary type) {

                return new TypeName(strategy.binaryTypeName());
            }

            @Override
            public Type<?> visitDecimal(final UseDecimal type) {

                return new TypeName(strategy.decimalTypeName());
            }

            @Override
            public Type<?> visitDate(final UseDate type) {

                return new TypeName(strategy.dateTypeName());
            }

            @Override
            public Type<?> visitDateTime(final UseDateTime type) {

                return new TypeName(strategy.dateTimeTypeName());
            }

            @Override
            public Type<?> visitView(final UseView type) {

                return new TypeName(strategy.inputTypeName(type.getSchema()));
            }

            @Override
            public Type<?> visitAny(final UseAny type) {

                return new TypeName(strategy.anyTypeName());
            }
        });
    }

    private Map<String, Use<?>> mapTypes(final InstanceSchema schema) {

        final Map<String, Use<?>> mapTypes = new HashMap<>();
        schema.getDeclaredProperties().forEach((k, v) -> {
            v.typeOf().visit(new Use.Visitor.Defaulting<Void>() {

                @Override
                public <T> Void visitDefault(final Use<T> type) {

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
                    mapTypes.put(strategy.mapEntryTypeName(type.getType()), type.getType());
                    return null;
                }
            });
        });
        return mapTypes;
    }

}
