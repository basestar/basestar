package io.basestar.schema.layout;

import io.basestar.schema.Property;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseStruct;
import io.basestar.schema.util.Casing;
import io.basestar.util.Name;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CasingLayout implements Layout {

    @Getter
    private final Layout baseLayout;

    private final Casing casing;

    public CasingLayout(final Layout baseLayout, final Casing casing) {

        this.baseLayout = baseLayout;
        this.casing = casing;
    }

    @Override
    public Map<String, Use<?>> layoutSchema(final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Use<?>> result = new HashMap<>();
        getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
            result.put(casing.apply(name), layoutSchema(type, branches.get(name)));
        });
        return result;
    }

    private Use<?> layoutSchema(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Transforming() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Use<?> transform(final Use<?> type, final Set<Name> expand) {

                return layoutSchema(type, expand);
            }

            @Override
            public Use<?> visitStruct(final UseStruct type) {

                final Map<String, Use<?>> props = new HashMap<>();
                boolean changed = false;
                for(final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
                    final String name = casing.apply(entry.getKey());
                    changed = changed || !name.equals(entry.getKey());
                }
                if(changed) {
                    return UseStruct.from(props);
                } else {
                    return type;
                }
            }
        });
    }

    @Override
    public Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
            final Object value = object == null ? null : object.get(name);
            result.put(casing.apply(name), applyLayout(type, branches.get(name), value));
        });
        return result;
    }

    public Object applyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                final Map<String, Object> object = type.create(getValue());
                final Map<String, Set<Name>> branches = Name.branch(getExpand());
                final Map<String, Object> result = new HashMap<>();
                type.getSchema().getProperties().forEach((name, prop) -> {
                    final Object value = object == null ? null : object.get(name);
                    result.put(casing.apply(name), applyLayout(type, branches.get(name), value));
                });
                return result;
            }
        });
    }

    @Override
    public Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> result = new HashMap<>();
        getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
            final Object value = object == null ? null : object.get(casing.apply(name));
            result.put(name, unapplyLayout(type, branches.get(name), value));
        });
        return result;
    }

    public Object unapplyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor.TransformingValue() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Object getValue() {

                return value;
            }

            @Override
            public Object transform(final Use<?> type, final Set<Name> expand, final Object value) {

                return applyLayout(type, expand, value);
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                final Map<String, Object> object = type.create(getValue());
                final Map<String, Set<Name>> branches = Name.branch(getExpand());
                final Map<String, Object> result = new HashMap<>();
                type.getSchema().getProperties().forEach((name, prop) -> {
                    final Object value = object == null ? null : object.get(casing.apply(name));
                    result.put(name, applyLayout(type, branches.get(name), value));
                });
                return result;
            }
        });
    }
}
