package io.basestar.schema.layout;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.use.*;
import io.basestar.util.Name;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JsonAnyLayout implements Layout {

    private final Layout base;

    private final ObjectMapper objectMapper;

    public JsonAnyLayout(final Layout base) {

        this.base = base;
        this.objectMapper = new ObjectMapper();
    }

    public JsonAnyLayout(final Layout base, final ObjectMapper objectMapper) {

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

                return type.transform(this::visit);
            }

            @Override
            public Use<?> visitAny(final UseAny type) {

                return UseString.DEFAULT;
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return null;
    }

    @Override
    public Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return null;
    }
}
