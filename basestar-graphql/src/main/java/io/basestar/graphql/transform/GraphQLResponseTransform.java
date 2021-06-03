package io.basestar.graphql.transform;

import io.basestar.graphql.GraphQLStrategy;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public interface GraphQLResponseTransform {

    Map<String, Object> toResponsePage(InstanceSchema schema, Page<? extends Map<String, Object>> input);

    Map<String, Object> toResponse(InstanceSchema schema, Map<String, Object> input);

    @Slf4j
    @RequiredArgsConstructor
    class Default implements GraphQLResponseTransform {

        private final GraphQLStrategy strategy;

        @Override
        public Map<String, Object> toResponsePage(final InstanceSchema schema, final Page<? extends Map<String, Object>> input) {

            log.debug("Transforming GQL response page {}", input);
            final Map<String, Object> result = new HashMap<>();
            result.put(strategy.pageItemsFieldName(), input.map(v -> toResponse(schema, v)).getItems());
            if (input.hasMore()) {
                result.put(strategy.pagePagingFieldName(), input.getPaging().toString());
            }
            final Page.Stats stats = input.getStats();
            if (stats != null) {
                if (stats.getTotal() != null) {
                    result.put(strategy.pageTotalFieldName(), stats.getTotal());
                }
                if (stats.getApproxTotal() != null) {
                    result.put(strategy.pageApproxTotalFieldName(), stats.getApproxTotal());
                }
            }
            return result;
        }

        @Override
        public Map<String, Object> toResponse(final InstanceSchema schema, final Map<String, Object> input) {

            if (input == null) {
                return null;
            } else {
                final InstanceSchema resolvedSchema;
                final Name schemaName = Instance.getSchema(input);
                if (schemaName != null) {
                    resolvedSchema = schema.resolveExtended(schemaName);
                } else {
                    resolvedSchema = schema;
                }
                final Map<String, Object> result = new HashMap<>();
                resolvedSchema.metadataSchema().forEach((k, use) -> result.put(k, toResponse(use, input.get(k))));
                resolvedSchema.getMembers().forEach((k, prop) -> result.put(k, toResponse(prop.typeOf(), input.get(k))));
                return result;
            }
        }

        protected Object toResponse(final Use<?> type, final Object value) {

            if (value == null) {
                return null;
            } else {
                return type.visit(new Use.Visitor.Defaulting<Object>() {

                    @Override
                    public <T> Object visitDefault(final Use<T> type) {

                        return type.create(value);
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public Object visitRef(final UseRef type) {

                        return toResponse(type.getSchema(), (Map<String, Object>) value);
                    }

                    @Override
                    public <T> Object visitArray(final UseArray<T> type) {

                        return ((Collection<?>) value).stream()
                                .map(v -> toResponse(type.getType(), v))
                                .collect(Collectors.toList());
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Object visitPage(final UsePage<T> type) {

                        final Use<T> itemType = type.getType();
                        if(itemType instanceof UseLinkable) {
                            final LinkableSchema schema = ((UseLinkable) itemType).getSchema();
                            return toResponsePage(schema, (Page<? extends Map<String, Object>>)value);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    }

                    @Override
                    public <T> Object visitSet(final UseSet<T> type) {

                        return ((Collection<?>) value).stream()
                                .map(v -> toResponse(type.getType(), v))
                                .collect(Collectors.toSet());
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Object visitMap(final UseMap<T> type) {

                        return ((Map<String, ?>) value).entrySet().stream()
                                .map(e -> {
                                    final Map<String, Object> result = new HashMap<>();
                                    result.put(GraphQLUtils.MAP_KEY, e.getKey());
                                    result.put(GraphQLUtils.MAP_VALUE, toResponse(type.getType(), e.getValue()));
                                    return result;
                                })
                                .collect(Collectors.toList());
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public Object visitStruct(final UseStruct type) {

                        return toResponse(type.getSchema(), (Map<String, Object>) value);
                    }

                    @Override
                    public <T> Object visitStringLike(final UseStringLike<T> type) {

                        return type.toString(type.create(value));
                    }
                });
            }
        }


//        @SuppressWarnings("unchecked")
//        public static Map<String, Object> toResponse(final GraphQLStrategy strategy, final InstanceSchema schema, final SelectionSet selections, final Map<String, Object> input) {
//
//            if (input == null) {
//                return null;
//            } else {
//                final Map<String, Object> result = new HashMap<>();
//
//                schema.metadataSchema().forEach((k, use) -> {
//                    if (selected(selections, k)) {
//                        result.put(k, toResponse(strategy, use, select(selections, k), input.get(k)));
//                    }
//                });
//                schema.getMembers().forEach((k, prop) -> {
//                    if (selected(selections, k)) {
//                        result.put(k, toResponse(strategy, prop.getType(), select(selections, k), input.get(k)));
//                    }
//                });
////            if (schema instanceof Link.Resolver) {
////                ((Link.Resolver) schema).getLinks().forEach((k, link) -> {
////                    if (selected(selections, k)) {
////                        final List<Map<String, Object>> values = (List<Map<String, Object>>) input.get(k);
////                        if (values != null) {
////                            result.put(k, values.stream().map(value -> toResponse(strategy, link.getSchema(), select(selections, k), value))
////                                    .collect(Collectors.toList()));
////                        }
////                    }
////                });
////            }
//                return result;
//            }
//        }

//        public static Object toResponse(final GraphQLStrategy strategy, final Use<?> type, final SelectionSet selections, final Object value) {
//
//            if (value == null) {
//                return null;
//            } else {
//                return type.visit(new Use.Visitor.Defaulting<Object>() {
//
//                    @Override
//                    public <T> Object visitDefault(final Use<T> type) {
//
//                        return type.create(value);
//                    }
//
//                    @Override
//                    @SuppressWarnings("unchecked")
//                    public Object visitRef(final UseRef type) {
//
//                        return toResponse(strategy, type.getSchema(), selections, (Map<String, Object>) value);
//                    }
//
//                    @Override
//                    public <T> Object visitArray(final UseArray<T> type) {
//
//                        return ((Collection<?>) value).stream()
//                                .map(v -> toResponse(strategy, type.getType(), selections, v))
//                                .collect(Collectors.toList());
//                    }
//
//                    @Override
//                    public <T> Object visitSet(final UseSet<T> type) {
//
//                        return ((Collection<?>) value).stream()
//                                .map(v -> toResponse(strategy, type.getType(), selections, v))
//                                .collect(Collectors.toSet());
//                    }
//
//                    @Override
//                    @SuppressWarnings("unchecked")
//                    public <T> Object visitMap(final UseMap<T> type) {
//
//                        return ((Map<String, ?>) value).entrySet().stream()
//                                .map(e -> {
//                                    final Map<String, Object> result = new HashMap<>();
//                                    if (selected(selections, MAP_KEY)) {
//                                        result.put(MAP_KEY, e.getKey());
//                                    }
//                                    if (selected(selections, MAP_VALUE)) {
//                                        result.put(MAP_VALUE, toResponse(strategy, type.getType(), select(selections, MAP_VALUE), e.getValue()));
//                                    }
//                                    return result;
//                                })
//                                .collect(Collectors.toList());
//                    }
//
//                    @Override
//                    public <T> Object visitPage(final UsePage<T> type) {
//
//                        return toPage(strategy, (Page<?>) value);
//                    }
//
//                    @Override
//                    @SuppressWarnings("unchecked")
//                    public Object visitStruct(final UseStruct type) {
//
//                        return toResponse(strategy, type.getSchema(), selections, (Map<String, Object>) value);
//                    }
//
//                    @Override
//                    @SuppressWarnings("unchecked")
//                    public Object visitView(final UseView type) {
//
//                        return toResponse(strategy, type.getSchema(), selections, (Map<String, Object>) value);
//                    }
//
//                    @Override
//                    public Object visitBinary(final UseBinary type) {
//
//                        return BaseEncoding.base64().encode(type.create(value));
//                    }
//
//                    @Override
//                    public <T> Object visitStringLike(final UseStringLike<T> type) {
//
//                        return type.create(value).toString();
//                    }
//
//                    @Override
//                    public Object visitAny(final UseAny type) {
//
//                        return value;
//                    }
//                });
//            }
//        }
    }
}
