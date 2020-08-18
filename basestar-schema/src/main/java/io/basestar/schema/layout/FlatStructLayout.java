package io.basestar.schema.layout;

import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseOptional;
import io.basestar.schema.use.UseStruct;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
        this.delimiter = Nullsafe.orDefault(delimiter, Reserved.PREFIX);
    }

    private String flatName(final Name qualifiedName) {

        return qualifiedName.toString(delimiter);
    }

    @Override
    public Map<String, Use<?>> layout(final Set<Name> expand) {

        return schema(Name.empty(), base.layout(expand), expand, false);
    }

    private Map<String, Use<?>> schema(final Name qualifiedName, final Map<String, Use<?>> schema, final Set<Name> expand, final boolean optional) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Use<?>> result = new HashMap<>();
        schema.forEach((name, type) -> {
            result.putAll(schema(qualifiedName.with(name), type, branches.get(name), optional));
        });
        return result;
    }

    private Map<String, Use<?>> schema(final Name qualifiedName, final Use<?> type, final Set<Name> expand, final boolean optional) {

        return type.visit(new Use.Visitor.Defaulting<Map<String, Use<?>>>() {

            @Override
            public <T> Map<String, Use<?>> visitDefault(final Use<T> type) {

                return Collections.singletonMap(flatName(qualifiedName), type.optional(optional));
            }

            @Override
            public <T> Map<String, Use<?>> visitOptional(final UseOptional<T> type) {

                return schema(qualifiedName, type.getType(), expand, true);
            }

            @Override
            public Map<String, Use<?>> visitStruct(final UseStruct type) {

                return schema(qualifiedName, type.getSchema().layout(expand), expand, optional);
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return apply(Name.empty(), base.layout(expand), expand, object);
    }

    private Map<String, Object> apply(final Name qualifiedName, final Map<String, Use<?>> schema, final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        schema.forEach((name, type) -> {
            final Object value = object == null ? null : object.get(name);
            result.putAll(apply(qualifiedName.with(name), type,branches.get(name), value));
        });
        return result;
    }

    private Map<String, Object> apply(final Name qualifiedName, final Use<?> type, final Set<Name> expand, final Object value) {

        return type.visit(new Use.Visitor.Defaulting<Map<String, Object>>() {

            @Override
            public <T> Map<String, Object> visitDefault(final Use<T> type) {

                return Collections.singletonMap(flatName(qualifiedName), type.create(value));
            }

            @Override
            public Map<String, Object> visitStruct(final UseStruct type) {

                return apply(qualifiedName, type.getSchema().layout(expand), expand, type.create(value));
            }
        });
    }

    @Override
    public Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        return unapply(Name.empty(), base.layout(expand), expand, object);
    }

    private Map<String, Object> unapply(final Name qualifiedName, final Map<String, Use<?>> schema, final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        schema.forEach((name, type) -> {
            result.put(name, unapply(qualifiedName.with(name), type, branches.get(name), object));
        });
        return result;
    }

    private Object unapply(final Name qualifiedName, final Use<?> type, final Set<Name> expand, final Map<String, Object> object) {

        return type.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitDefault(final Use<T> type) {

                final String name = flatName(qualifiedName);
                return type.create(object.get(name));
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                return unapply(qualifiedName, type.getSchema().layout(expand), expand, object);
            }
        });
    }
}
