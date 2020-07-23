package io.basestar.storage.sql.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.jooq.DataType;
import org.jooq.Record;
import org.jooq.Table;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public interface ColumnMapper<T> {

    Map<Name, DataType<?>> toColumns(Name name);

    Map<String, Object> toSQLValues(String name, T value);

    T fromSQLValues(String name, Map<String, Object> values);

    Name resolve(Name name);

    Table<Record> joined(Name name, Table<Record> source, Function<ObjectSchema, Table<Record>> resolver);

    interface Single<T, V> extends ColumnMapper<T> {

        DataType<V> dataType();

        T marshall(V value);

        V unmarshall(T value);

        @Override
        default Map<Name, DataType<?>> toColumns(final Name name) {

            return Collections.singletonMap(name, dataType());
        }

        @Override
        default Map<String, Object> toSQLValues(final String name, final T value) {

            if(value == null) {
                return Collections.singletonMap(name, null);
            } else {
                final V converted = dataType().convert(unmarshall(value));
                return Collections.singletonMap(name, converted);
            }
        }

        @Override
        default T fromSQLValues(final String name, final Map<String, Object> values) {

            final Object value = values.get(name);
            if(value == null) {
                return null;
            } else {
                final V converted = dataType().convert(value);
                return marshall(converted);
            }
        }

        @Override
        default Name resolve(final Name name) {

            if(name.size() == 1) {
                return name;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        default Table<Record> joined(final Name name, final Table<Record> source, final Function<ObjectSchema, Table<Record>> resolver) {

            return source;
        }
    }

    @RequiredArgsConstructor
    class Simple<T, V> implements Single<T, V> {

        private final DataType<V> dataType;

        private final Function<V, T> marshaller;

        private final Function<T, V> unmarshaller;

        @Override
        public DataType<V> dataType() {

            return dataType;
        }

        @Override
        public T marshall(final V value) {

            return marshaller.apply(value);
        }

        @Override
        public V unmarshall(final T value) {

            return unmarshaller.apply(value);
        }
    }

    @RequiredArgsConstructor
    class Json<T> implements Single<T, String> {

        private static final ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new BasestarModule());

        private final DataType<String> dataType;

        private final Function<Object, T> marshaller;

        @Override
        public DataType<String> dataType() {

            return dataType;
        }

        @Override
        public T marshall(final String value) {

            try {
                return marshaller.apply(objectMapper.readValue(value, Object.class));
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String unmarshall(final T value) {

            try {
                return objectMapper.writeValueAsString(value);
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Name resolve(final Name name) {

            throw new IllegalStateException();
        }
    }

    static <T, V> Simple<T, V> simple(final DataType<V> dataType, final Function<V, T> marshaller, final Function<T, V> unmarshaller) {

        return new Simple<>(dataType, marshaller, unmarshaller);
    }

    @SuppressWarnings("unchecked")
    static <T, V> Simple<T, V> simple(final DataType<V> dataType, final Use<T> type) {

        return simple(dataType, type::create, v -> (V)type.create(v));
    }

    static <T> ColumnMapper<T> json(final DataType<String> dataType, final Use<T> type) {

        return new Json<>(dataType, type::create);
    }
}
