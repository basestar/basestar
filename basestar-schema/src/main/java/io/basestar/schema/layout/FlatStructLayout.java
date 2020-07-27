package io.basestar.schema.layout;

import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseNullable;
import io.basestar.schema.use.UseStruct;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Layout transformation that converts structs to top-level properties with delimited names.
 *
 * Structs within structs will be recursively flattened. Structs within collections/maps will not be changed.
 */

public class FlatStructLayout implements Layout {

    private final Layout base;

    private final String delimiter;

    public FlatStructLayout(final Layout base) {

        this(base, null);
    }

    public FlatStructLayout(final Layout base, final String delimiter) {

        this.base = base;
        this.delimiter = Nullsafe.option(delimiter, Reserved.PREFIX);
    }

    private String flatName(final Name qualifiedName) {

        return qualifiedName.toString(delimiter);
    }

    @Override
    public Map<String, Use<?>> layout() {

        return schema(Name.empty(), base.layout(), false);
    }

    private Map<String, Use<?>> schema(final Name qualifiedName, final Map<String, Use<?>> schema, final boolean nullable) {

        final Map<String, Use<?>> result = new HashMap<>();
        schema.forEach((name, type) -> {
            result.putAll(schema(qualifiedName.with(name), type, nullable));
        });
        return result;
    }

    private Map<String, Use<?>> schema(final Name qualifiedName, final Use<?> type, final boolean nullable) {

        return type.visit(new Use.Visitor.Defaulting<Map<String, Use<?>>>() {

            @Override
            public Map<String, Use<?>> visitDefault(final Use<?> type) {

                return Collections.singletonMap(flatName(qualifiedName), type.nullable(nullable));
            }

            @Override
            public <T> Map<String, Use<?>> visitNullable(final UseNullable<T> type) {

                return schema(qualifiedName, type.getType(), true);
            }

            @Override
            public Map<String, Use<?>> visitStruct(final UseStruct type) {

                return schema(qualifiedName, type.getSchema().layout(), nullable);
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Map<String, Object> object) {

        return apply(Name.empty(), base.layout(), object);
    }

    private Map<String, Object> apply(final Name qualifiedName, final Map<String, Use<?>> schema, final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        schema.forEach((name, type) -> {
            final Object value = object == null ? null : object.get(name);
            result.putAll(apply(qualifiedName.with(name), type, value));
        });
        return result;
    }

    private Map<String, Object> apply(final Name qualifiedName, final Use<?> type, final Object value) {

        return type.visit(new Use.Visitor.Defaulting<Map<String, Object>>() {

            @Override
            public Map<String, Object> visitDefault(final Use<?> type) {

                return Collections.singletonMap(flatName(qualifiedName), type.create(value));
            }

            @Override
            public Map<String, Object> visitStruct(final UseStruct type) {

                return apply(qualifiedName, type.getSchema().layout(), type.create(value));
            }
        });
    }

    @Override
    public Map<String, Object> unapplyLayout(final Map<String, Object> object) {

        return unapply(Name.empty(), base.layout(), object);
    }

    private Map<String, Object> unapply(final Name qualifiedName, final Map<String, Use<?>> schema, final Map<String, Object> object) {

        final Map<String, Object> result = new HashMap<>();
        schema.forEach((name, type) -> {
            result.put(name, unapply(qualifiedName.with(name), type, object));
        });
        return result;
    }

    private Object unapply(final Name qualifiedName, final Use<?> type, final Map<String, Object> object) {

        return type.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public Object visitDefault(final Use<?> type) {

                final String name = flatName(qualifiedName);
                return type.create(object.get(name));
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                return unapply(qualifiedName, type.getSchema().layout(), object);
            }
        });
    }
}
