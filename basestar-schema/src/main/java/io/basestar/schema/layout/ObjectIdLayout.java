package io.basestar.schema.layout;

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.*;

import java.util.*;

/**
 * Layout transformation that converts object and view-record references to flat string fields containing the target id.
 *
 * This transformation works recursively through collections, maps and structs
 */

public class ObjectIdLayout implements Layout {

    private final Layout base;

    public ObjectIdLayout(final Layout base) {

        this.base = base;
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

                return layout(type, true);
            }

            @Override
            public Use<?> visitLinkable(final UseLinkable type) {

                return UseString.DEFAULT.nullable(nullable);
            }

            @Override
            public <T> Use<?> visitArray(final UseArray<T> type) {

                return new UseArray<>(layout(type.getType(), false)).nullable(nullable);
            }

            @Override
            public <T> Use<?> visitSet(final UseSet<T> type) {

                return new UseSet<>(layout(type.getType(), false)).nullable(nullable);
            }

            @Override
            public <T> Use<?> visitMap(final UseMap<T> type) {

                return new UseMap<>(layout(type.getType(), false)).nullable(nullable);
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

                return type;
            }

            @Override
            public Object visitObject(final UseObject type) {

                return Instance.getId(type.create(value));
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                final List<Object> result = new ArrayList<>();
                type.create(value).forEach(v -> result.add(applyLayout(type.getType(), v)));
                return result;
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                final Set<Object> result = new HashSet<>();
                type.create(value).forEach(v -> result.add(applyLayout(type.getType(), v)));
                return result;
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                final Map<String, Object> result = new HashMap<>();
                type.create(value).forEach((k, v) -> result.put(k, applyLayout(type.getType(), v)));
                return result;
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
            public Object visitObject(final UseObject type) {

                return ObjectSchema.ref((String)value);
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                final List<Object> result = new ArrayList<>();
                type.create(value).forEach(v -> result.add(unapplyLayout(type.getType(), v)));
                return result;
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                final Set<Object> result = new HashSet<>();
                type.create(value).forEach(v -> result.add(unapplyLayout(type.getType(), v)));
                return result;
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                final Map<String, Object> result = new HashMap<>();
                type.create(value).forEach((k, v) -> result.put(k, unapplyLayout(type.getType(), v)));
                return result;
            }
        });
    }
}
