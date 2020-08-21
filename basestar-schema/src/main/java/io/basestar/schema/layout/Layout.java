package io.basestar.schema.layout;

import io.basestar.schema.use.Use;
import io.basestar.util.Name;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface Layout extends Serializable {

    Map<String, Use<?>> layoutSchema(Set<Name> expand);

    Map<String, Object> applyLayout(Set<Name> expand, Map<String, Object> object);

    Map<String, Object> unapplyLayout(Set<Name> expand, Map<String, Object> object);

    interface Transforming extends Layout {

        Layout getBaseLayout();
    }

    interface Simple extends Transforming {

        Use<?> layoutSchema(Use<?> type, Set<Name> expand);

        Object applyLayout(Use<?> type, Set<Name> expand, Object value);

        Object unapplyLayout(Use<?> type, Set<Name> expand, Object value);

        @Override
        default Map<String, Use<?>> layoutSchema(final Set<Name> expand) {

            final Map<String, Set<Name>> branches = Name.branch(expand);
            final Map<String, Use<?>> result = new HashMap<>();
            getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
                result.put(name, layoutSchema(type, branches.get(name)));
            });
            return result;
        }

        @Override
        default Map<String, Object> applyLayout(final Set<Name> expand, final Map<String, Object> object) {

            final Map<String, Set<Name>> branches = Name.branch(expand);
            final Map<String, Object> result = new HashMap<>();
            getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
                final Object value = object == null ? null : object.get(name);
                result.put(name, applyLayout(type, branches.get(name), value));
            });
            return result;
        }

        @Override
        default Map<String, Object> unapplyLayout(final Set<Name> expand, final Map<String, Object> object) {

            final Map<String, Set<Name>> branches = Name.branch(expand);
            final Map<String, Object> result = new HashMap<>();
            getBaseLayout().layoutSchema(expand).forEach((name, type) -> {
                final Object value = object == null ? null : object.get(name);
                result.put(name, unapplyLayout(type, branches.get(name), value));
            });
            return result;
        }
    }
}
