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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import graphql.GraphQLContext;
import graphql.execution.ExecutionContext;
import graphql.language.*;
import io.basestar.auth.Caller;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.Path;

import java.util.*;
import java.util.stream.Collectors;

public class GraphQLUtils {

    public static final String ID_TYPE = "ID";

    public static final String STRING_TYPE = "String";

    public static final String INT_TYPE = "Int";

    public static final String FLOAT_TYPE = "Float";

    public static final String BOOLEAN_TYPE = "Boolean";

    public static final String MAP_KEY = "key";

    public static final String MAP_VALUE = "value";

    public static final String QUERY_TYPE = "Query";

    public static final String MUTATION_TYPE = "Mutation";

    private static final SelectionSet EMPTY_SELECTION = SelectionSet.newSelectionSet().build();

    public static Set<Path> paths(final InstanceSchema schema, final SelectionSet selections) {

        return selections.getSelections().stream()
                .flatMap(v -> paths(schema, v).stream())
                .collect(Collectors.toSet());
    }

    public static Set<Path> paths(final InstanceSchema schema, final Selection<?> selection) {

        if(selection instanceof Field) {
            final Field field = (Field)selection;
            final String name = field.getName();

            if(schema.metadataSchema().containsKey(name)) {
                return Collections.singleton(Path.of(name));
            }

            if(schema instanceof Link.Resolver) {
                final Link link = ((Link.Resolver) schema).getLink(name, true);
                if(link != null) {
                    return paths(link.getSchema(), field.getSelectionSet()).stream()
                            .map(v -> Path.of(name).with(v)).collect(Collectors.toSet());
                }
            }
            final Property property = schema.requireProperty(name, true);
            return paths(property.getType(), Path.of(name), field.getSelectionSet());

        } else{
            throw new IllegalStateException();
        }
    }

    protected static Set<Path> paths(final Use<?> type, final Path parent, final SelectionSet selections) {

        return type.visit(new Use.Visitor<Set<Path>>() {
            @Override
            public Set<Path> visitBoolean(final UseBoolean type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Path> visitInteger(final UseInteger type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Path> visitNumber(final UseNumber type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Path> visitString(final UseString type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Path> visitEnum(final UseEnum type) {

                return ImmutableSet.of(parent);
            }

            @Override
            public Set<Path> visitRef(final UseRef type) {

                return paths(type.getSchema(), selections).stream()
                        .map(parent::with).collect(Collectors.toSet());
            }

            @Override
            public <T> Set<Path> visitArray(final UseArray<T> type) {

                return paths(type.getType(), parent, selections);
            }

            @Override
            public <T> Set<Path> visitSet(final UseSet<T> type) {

                return paths(type.getType(), parent, selections);
            }

            @Override
            public <T> Set<Path> visitMap(final UseMap<T> type) {

                final Set<Path> paths = new HashSet<>();
                for(final Selection<?> selection : selections.getSelections()) {
                    final Field field = (Field)selection;
                    final String name = field.getName();
                    if(MAP_KEY.equals(name)) {
                        paths.add(parent.with("*"));
                    } else if(MAP_VALUE.equals(name)) {
                        paths.addAll(paths(type.getType(), parent.with("*"), field.getSelectionSet()));
                    }
                }
                return paths;
            }

            @Override
            public Set<Path> visitStruct(final UseStruct type) {

                return paths(type.getSchema(), selections).stream()
                        .map(parent::with).collect(Collectors.toSet());
            }

            @Override
            public Set<Path> visitBinary(final UseBinary type) {

                return ImmutableSet.of(parent);
            }
        });
    }

    public static Map<String, Object> fromRequest(final InstanceSchema schema, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.getProperties().forEach((k, prop) -> result.put(k, fromRequest(prop.getType(), input.get(k))));
            return result;
        }
    }

    protected static Object fromRequest(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                public Object visitRef(final UseRef type) {

                    return type.create(value);
                }

                @Override
                public <T> Object visitArray(final UseArray<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> fromRequest(type.getType(), v))
                            .collect(Collectors.toList());
                }

                @Override
                public <T> Object visitSet(final UseSet<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> fromRequest(type.getType(), v))
                            .collect(Collectors.toSet());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitMap(final UseMap<T> type) {

                    final Map<String, Object> result = new HashMap<>();
                    ((Collection<Map<String, ?>>)value).forEach(v -> {
                        final String key = (String)v.get(GraphQLUtils.MAP_KEY);
                        final Object value = fromRequest(type.getType(), v.get(GraphQLUtils.MAP_VALUE));
                        result.put(key, value);
                    });
                    return result;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    return fromRequest(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    return type.create(BaseEncoding.base64().decode(value.toString()));
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toResponse(final InstanceSchema schema, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.metadataSchema().forEach((k, use) -> result.put(k, toResponse(use, input.get(k))));
            schema.getProperties().forEach((k, prop) -> result.put(k, toResponse(prop.getType(), input.get(k))));
            if (schema instanceof Link.Resolver) {
                ((Link.Resolver) schema).getLinks().forEach((k, link) -> {
                    final List<Map<String, Object>> values = (List<Map<String, Object>>) input.get(k);
                    if (values != null) {
                        result.put(k, values.stream().map(value -> toResponse(link.getSchema(), value))
                                .collect(Collectors.toList()));
                    }
                });
            }
            return result;
        }
    }

    public static Object toResponse(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitRef(final UseRef type) {

                    return toResponse(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                public <T> Object visitArray(final UseArray<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), v))
                            .collect(Collectors.toList());
                }

                @Override
                public <T> Object visitSet(final UseSet<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), v))
                            .collect(Collectors.toSet());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitMap(final UseMap<T> type) {

                    return ((Map<String, ?>)value).entrySet().stream()
                            .map(e -> {
                                final Map<String, Object> result = new HashMap<>();
                                result.put(MAP_KEY, e.getKey());
                                result.put(MAP_VALUE, toResponse(type.getType(), e.getValue()));
                                return result;
                            })
                            .collect(Collectors.toList());
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    return toResponse(type.getSchema(), (Map<String, Object>)value);
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    return BaseEncoding.base64().encode(type.create(value));
                }
            });
        }
    }

    public static Map<String, Object> fromInput(final ExecutionContext context, final InstanceSchema schema, final ObjectValue input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.getProperties().forEach((k, prop) -> result.put(k, fromInput(context, prop.getType(), get(input, k))));
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromInput(final ExecutionContext context, final Use<T> type, final Value<?> value) {

        if(value == null) {
            return null;
        } else if(value instanceof VariableReference) {

            final String name = ((VariableReference) value).getName();
            return fromInput(type, context.getVariables().get(name));

        } else {

            return (T)type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    if(value instanceof BooleanValue) {
                        return ((BooleanValue) value).isValue();
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    if(value instanceof IntValue) {
                        return ((IntValue) value).getValue().longValue();
                    } else if(value instanceof FloatValue) {
                        return ((FloatValue) value).getValue().longValue();
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    if(value instanceof IntValue) {
                        return ((IntValue) value).getValue().doubleValue();
                    } else if(value instanceof FloatValue) {
                        return ((FloatValue) value).getValue().doubleValue();
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitString(final UseString type) {

                    if(value instanceof StringValue) {
                        return ((StringValue) value).getValue();
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    if(value instanceof EnumValue) {
                        return ((EnumValue) value).getName();
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitRef(final UseRef type) {

                    if(value instanceof ObjectValue) {
                        final String id = fromInput(context, UseString.DEFAULT, get((ObjectValue)value, Reserved.ID));
                        return ObjectSchema.ref(id);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T2> Object visitArray(final UseArray<T2> type) {

                    if(value instanceof ArrayValue) {
                        return ((ArrayValue)value).getValues().stream()
                                .map(v -> fromInput(context, type.getType(), v))
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T2> Object visitSet(final UseSet<T2> type) {

                    if(value instanceof ArrayValue) {
                        return ((ArrayValue)value).getValues().stream()
                                .map(v -> fromInput(context, type.getType(), v))
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T2> Object visitMap(final UseMap<T2> type) {

                    if(value instanceof ArrayValue) {
                        final Map<String, Object> result = new HashMap<>();
                        ((ArrayValue)value).getValues().forEach(v -> {
                            final ObjectValue entry = (ObjectValue)v;
                            final Value<?> keyValue = get(entry, GraphQLUtils.MAP_KEY);
                            final String key = ((StringValue)keyValue).getValue();
                            final Object value = fromInput(context, type.getType(), get(entry, GraphQLUtils.MAP_VALUE));
                            result.put(key, value);
                        });
                        return result;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitStruct(final UseStruct type) {

                    if(value instanceof ObjectValue) {
                        return fromInput(context, type.getSchema(), (ObjectValue) value);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    if(value instanceof StringValue) {
                        return BaseEncoding.base64().decode(((StringValue) value).getValue());
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    public static Map<String, Object> fromInput(final InstanceSchema schema, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();
            schema.getProperties().forEach((k, prop) -> result.put(k, fromInput(prop.getType(), input.get(k))));
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromInput(final Use<T> type, final Object value) {

        if(value == null) {
            return null;
        } else {

            return (T)type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitRef(final UseRef type) {

                    if(value instanceof Map) {
                        return ObjectSchema.ref(Instance.getId((Map<String, Object>) value));
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T2> Object visitArray(final UseArray<T2> type) {

                    if(value instanceof Collection) {
                        return ((Collection<?>)value).stream()
                                .map(v -> fromInput(type.getType(), v))
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T2> Object visitSet(final UseSet<T2> type) {

                    if(value instanceof Collection) {
                        return ((Collection<?>)value).stream()
                                .map(v -> fromInput(type.getType(), v))
                                .collect(Collectors.toList());
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T2> Object visitMap(final UseMap<T2> type) {

                    if(value instanceof Collection) {
                        final Map<String, Object> result = new HashMap<>();
                        ((Collection<Map<String, ?>>)value).forEach(entry -> {
                            final String key = (String)entry.get(GraphQLUtils.MAP_KEY);
                            final Object value = fromInput(type.getType(), entry.get(GraphQLUtils.MAP_VALUE));
                            result.put(key, value);
                        });
                        return result;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    if(value instanceof Map) {
                        return fromInput(type.getSchema(), (Map<String, Object>) value);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    if(value instanceof String) {
                        return BaseEncoding.base64().decode((String) value);
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toResponse(final InstanceSchema schema, final SelectionSet selections, final Map<String, Object> input) {

        if(input == null) {
            return null;
        } else {
            final Map<String, Object> result = new HashMap<>();

            schema.metadataSchema().forEach((k, use) -> {
                if(selected(selections, k)) {
                    result.put(k, toResponse(use, select(selections, k), input.get(k)));
                }
            });
            schema.getProperties().forEach((k, prop) -> {
                if(selected(selections, k)) {
                    result.put(k, toResponse(prop.getType(), select(selections, k), input.get(k)));
                }
            });
            if (schema instanceof Link.Resolver) {
                ((Link.Resolver) schema).getLinks().forEach((k, link) -> {
                    if(selected(selections, k)) {
                        final List<Map<String, Object>> values = (List<Map<String, Object>>) input.get(k);
                        if (values != null) {
                            result.put(k, values.stream().map(value -> toResponse(link.getSchema(), select(selections, k), value))
                                    .collect(Collectors.toList()));
                        }
                    }
                });
            }
            return result;
        }
    }

    public static Object toResponse(final Use<?> type, final SelectionSet selections, final Object value) {

        if(value == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor<Object>() {
                @Override
                public Object visitBoolean(final UseBoolean type) {

                    return type.create(value);
                }

                @Override
                public Object visitInteger(final UseInteger type) {

                    return type.create(value);
                }

                @Override
                public Object visitNumber(final UseNumber type) {

                    return type.create(value);
                }

                @Override
                public Object visitString(final UseString type) {

                    return type.create(value);
                }

                @Override
                public Object visitEnum(final UseEnum type) {

                    return type.create(value);
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitRef(final UseRef type) {

                    return toResponse(type.getSchema(), selections, (Map<String, Object>)value);
                }

                @Override
                public <T> Object visitArray(final UseArray<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), selections, v))
                            .collect(Collectors.toList());
                }

                @Override
                public <T> Object visitSet(final UseSet<T> type) {

                    return ((Collection<?>)value).stream()
                            .map(v -> toResponse(type.getType(), selections, v))
                            .collect(Collectors.toSet());
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Object visitMap(final UseMap<T> type) {

                    return ((Map<String, ?>)value).entrySet().stream()
                            .map(e -> {
                                final Map<String, Object> result = new HashMap<>();
                                if(selected(selections, MAP_KEY)) {
                                    result.put(MAP_KEY, e.getKey());
                                }
                                if(selected(selections, MAP_VALUE)) {
                                    result.put(MAP_VALUE, toResponse(type.getType(), select(selections, MAP_VALUE), e.getValue()));
                                }
                                return result;
                            })
                            .collect(Collectors.toList());
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object visitStruct(final UseStruct type) {

                    return toResponse(type.getSchema(), selections, (Map<String, Object>)value);
                }

                @Override
                public Object visitBinary(final UseBinary type) {

                    return BaseEncoding.base64().encode(type.create(value));
                }
            });
        }
    }

//    public static <T> T argValue(final ExecutionContext context, final Use<T> type, final Field field, final String name) {
//
//        return fromInput(context, type, argValue(field, name));
//    }
//
//    public static Map<String, Object> argInput(final ExecutionContext context, final InstanceSchema schema, final Field field, final String name) {
//
//        return fromInput(context, schema, (ObjectValue)argValue(field, name));
//    }
//
//    public static Map<String, Expression> argInputExpr(final ExecutionContext context, final ObjectSchema schema, final Field field, final String name) {
//
//        final ObjectValue object = (ObjectValue)argValue(field, name);
//        if(object == null) {
//            return null;
//        } else {
//            final Map<String, Expression> result = new HashMap<>();
//            schema.getProperties().forEach((k, prop) -> {
//                final Value<?> value = get(object, k);
//                if(value instanceof VariableReference) {
//                    final String var = ((VariableReference) value).getName();
//                    final String str = (String)context.getVariables().get(var);
//                    final Expression expr = Expression.parse(str);
//                    result.put(k, expr);
//                } else if(value instanceof StringValue) {
//                    final Expression expr = Expression.parse(((StringValue) value).getValue());
//                    result.put(k, expr);
//                }
//            });
//            return result;
//        }
//    }

    public static Value<?> argValue(final Field field, final String name) {

        final Argument arg = arg(field, name);
        return arg == null ? null : arg.getValue();
    }

    public static Argument arg(final Field field, final String name) {

        return field.getArguments().stream().filter(v -> v.getName().equals(name))
                .findFirst().orElse(null);
    }

    public static Value<?> get(final ObjectValue input, final String name) {

        return input.getObjectFields().stream()
                .filter(v -> v.getName().equals(name))
                .map(ObjectField::getValue)
                .findFirst().orElse(null);
    }

    private static SelectionSet select(final SelectionSet selections, final String k) {

        for(final Selection<?> v : selections.getSelections()) {
            if(v instanceof Field && ((Field) v).getName().equals(k)) {
                return ((Field) v).getSelectionSet();
            }
        }
        return EMPTY_SELECTION;
    }

    private static boolean selected(final SelectionSet selections, final String k) {

        if(selections == null) {
            return false;
        } else {
            return selections.getSelections().stream().anyMatch(v -> {
                if (v instanceof Field) {
                    return k.equals(((Field) v).getName());
                } else {
                    return false;
                }
            });
        }
    }

    public static Caller caller(final GraphQLContext context) {

        if(context != null) {
            final Caller caller = context.get("caller");
            if(caller != null) {
                return caller;
            }
        }
        // FIXME
        return Caller.SUPER;
    }
}
