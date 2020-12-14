package io.basestar.graphql.transform;

import io.basestar.graphql.GraphQLStrategy;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.use.*;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public interface GraphQLRequestTransform {

    Map<String, Object> fromRequest(InstanceSchema schema, Map<String, Object> input);

    @RequiredArgsConstructor
    class Default implements GraphQLRequestTransform {

        private final GraphQLStrategy strategy;

        @Override
        public Map<String, Object> fromRequest(final InstanceSchema schema, final Map<String, Object> input) {

            if (input == null) {
                return null;
            } else {
                final Map<String, Object> result = new HashMap<>();
                schema.getProperties().forEach((k, prop) -> {
                    if (input.containsKey(k)) {
                        result.put(k, fromRequest(prop.getType(), input.get(k)));
                    }
                });
                return result;
            }
        }

        protected Object fromRequest(final Use<?> type, final Object value) {

            if (value == null) {
                return null;
            } else {
                return type.visit(new Use.Visitor.Defaulting<Object>() {

                    @Override
                    public <T> Object visitDefault(final Use<T> type) {

                        return type.create(value);
                    }

                    @Override
                    public <T> Object visitArray(final UseArray<T> type) {

                        return ((Collection<?>) value).stream()
                                .map(v -> fromRequest(type.getType(), v))
                                .collect(Collectors.toList());
                    }

                    @Override
                    public <T> Object visitSet(final UseSet<T> type) {

                        return ((Collection<?>) value).stream()
                                .map(v -> fromRequest(type.getType(), v))
                                .collect(Collectors.toSet());
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Object visitMap(final UseMap<T> type) {

                        final Map<String, Object> result = new HashMap<>();
                        ((Collection<Map<String, ?>>) value).forEach(v -> {
                            final String key = (String) v.get(GraphQLUtils.MAP_KEY);
                            final Object value = fromRequest(type.getType(), v.get(GraphQLUtils.MAP_VALUE));
                            result.put(key, value);
                        });
                        return result;
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public Object visitStruct(final UseStruct type) {

                        return fromRequest(type.getSchema(), (Map<String, Object>) value);
                    }
                });
            }
        }
    }
}
