package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Instance;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;
import org.jooq.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface PropertyMapping<T> {

    String getName();

    T fromSQLValues(Name alias, Map<Name, ?> values);

    Map<Name, Object> toSQLValues(Name alias, T value);

    //Map<Name, DataType<?>> dataTypes(Name alias);

    @Data
    class Simple<T> implements PropertyMapping<T> {

        private final String name;

        private final DataType<?> dataType;

        private final ValueTransform<T, ?> transform;

        public Simple(final String name, final DataType<?> dataType) {

            this(name, dataType, null);
        }

        public <S> Simple(final String name, final DataType<S> dataType, final ValueTransform<T, S> transform) {

            this.name = name;
            this.dataType = dataType;
            this.transform = transform;
        }


        @Override
        @SuppressWarnings("unchecked")
        public T fromSQLValues(final Name alias, final Map<Name, ?> values) {

            final Object raw = values.get(alias.with(name));
            if (transform != null) {
                return ((ValueTransform<T, Object>) transform).fromSQLValue(raw);
            } else {
                return (T) raw;
            }
        }

        @Override
        public Map<Name, Object> toSQLValues(final Name alias, final T value) {

            final Object transformed;
            if (transform != null) {
                transformed = transform.toSQLValue(value);
            } else {
                transformed = value;
            }
            return Immutable.map(alias.with(name), transformed);
        }
    }

    interface Flattened extends PropertyMapping<Instance> {

        String getName();

        List<PropertyMapping<?>> getMappings();

        @Override
        default Instance fromSQLValues(final Name alias, final Map<Name, ?> values) {

            final String name = getName();
            final List<PropertyMapping<?>> mappings = getMappings();
            final Map<String, Object> result = new HashMap<>();
            for (final PropertyMapping<?> mapping : mappings) {
                final Object value = mapping.fromSQLValues(alias.with(name), values);
                result.put(mapping.getName(), value);
            }
            return new Instance(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        default Map<Name, Object> toSQLValues(final Name alias, final Instance value) {

            final String name = getName();
            final List<PropertyMapping<?>> mappings = getMappings();
            final Map<Name, Object> result = new HashMap<>();
            for (final PropertyMapping<?> mapping : mappings) {
                result.putAll(((PropertyMapping<Object>) mapping).toSQLValues(alias.with(name), value.get(mapping.getName())));
            }
            return result;
        }
    }

    @Data
    class FlattenedStruct implements Flattened {

        private final String name;

        private final List<PropertyMapping<?>> mappings;

        public FlattenedStruct(final String name, final List<PropertyMapping<?>> mappings) {

            this.name = name;
            this.mappings = ImmutableList.copyOf(mappings);
        }
    }

    @Data
    class FlattenedRef implements Flattened {

        private final String name;

        private final List<PropertyMapping<?>> mappings;

        public FlattenedRef(final String name, final List<PropertyMapping<?>> mappings) {

            this.name = name;
            this.mappings = ImmutableList.copyOf(mappings);
        }

        @Override
        public Instance fromSQLValues(final Name alias, final Map<Name, ?> values) {

            final Instance instance = Flattened.super.fromSQLValues(alias, values);
            if (Instance.getId(instance) == null) {
                return null;
            } else {
                return instance;
            }
        }
    }
}
