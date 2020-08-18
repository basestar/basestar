package io.basestar.schema.layout;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.use.*;
import io.basestar.util.Name;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Layout transformation that converts collections and maps to JSON strings
 */

public class JsonContainerLayout implements Layout {

    private final Layout base;

    private final ObjectMapper objectMapper;

    public JsonContainerLayout(final Layout base) {

        this.base = base;
        this.objectMapper = new ObjectMapper();
    }

    public JsonContainerLayout(final Layout base, final ObjectMapper objectMapper) {

        this.base = base;
        this.objectMapper = objectMapper;
    }

    @Override
    public Map<String, Use<?>> layout(final Set<Name> expand) {

        final Map<String, Use<?>> result = new HashMap<>();
        base.layout(expand).forEach((name, type) -> {
            result.put(name, layout(type));
        });
        return result;
    }

    private Use<?> layout(final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Use<?>>() {

            @Override
            public <T> Use<?> visitDefault(final Use<T> type) {

                return type;
            }

            @Override
            public <T> Use<?> visitOptional(final UseOptional<T> type) {

                return layout(type.getType()).optional(true);
            }

            @Override
            public <T> Use<?> visitContainer(final UseContainer<T, ?> type) {

                return UseString.DEFAULT;
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        base.layout(expand).forEach((name, type) -> {
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
            public <T> Object visitDefault(final Use<T> type) {

                return type.create(value);
            }

            @Override
            public <T> Object visitContainer(final UseContainer<T, ?> type) {

                try {
                    return objectMapper.writeValueAsString(value);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    @Override
    public Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        base.layout(expand).forEach((name, type) -> {
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
            public <T> Object visitDefault(final Use<T> type) {

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
