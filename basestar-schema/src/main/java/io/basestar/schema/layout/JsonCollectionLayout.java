package io.basestar.schema.layout;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.use.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Layout transformation that converts collections and maps to JSON strings
 */

public class JsonCollectionLayout implements Layout {

    private final Layout base;

    private final ObjectMapper objectMapper;

    public JsonCollectionLayout(final Layout base) {

        this.base = base;
        this.objectMapper = new ObjectMapper();
    }

    public JsonCollectionLayout(final Layout base, final ObjectMapper objectMapper) {

        this.base = base;
        this.objectMapper = objectMapper;
    }

    @Override
    public Map<String, Use<?>> layout() {

        final Map<String, Use<?>> result = new HashMap<>();
        base.layout().forEach((name, type) -> {
            result.put(name, layout(type, false));
        });
        return result;
    }

    private Use<?> layout(final Use<?> type, final boolean nullable) {

        return type.visit(new Use.Visitor.Defaulting<Use<?>>() {

            @Override
            public Use<?> visitDefault(final Use<?> type) {

                return type;
            }

            @Override
            public <T> Use<?> visitNullable(final UseNullable<T> type) {

                return layout(type.getType(), true);
            }

            @Override
            public <T> Use<?> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                return UseString.DEFAULT.nullable(nullable);
            }

            @Override
            public <T> Use<?> visitMap(final UseMap<T> type) {

                return UseString.DEFAULT.nullable(nullable);
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        base.layout().forEach((name, type) -> {
            final Object value = object == null ? null : object.get(name);
            result.put(name, applyLayout(type, value));
        });
        return result;
    }

    private Object applyLayout(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public Object visitDefault(final Use<?> type) {

                return type.create(value);
            }

            @Override
            public <T> Object visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                try {
                    return objectMapper.writeValueAsString(value);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                try {
                    return objectMapper.writeValueAsString(value);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    @Override
    public Map<String, Object> unapplyLayout(final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        base.layout().forEach((name, type) -> {
            final Object value = object == null ? null : object.get(name);
            result.put(name, unapplyLayout(type, value));
        });
        return result;
    }

    private Object unapplyLayout(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public Object visitDefault(final Use<?> type) {

                return type.create(value);
            }

            @Override
            public <T> Object visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                try {
                    final Object result = objectMapper.readValue((String)value, Collection.class);
                    return type.create(result);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                try {
                    final Object result = objectMapper.readValue((String)value, Map.class);
                    return type.create(result);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }
}
