package io.basestar.storage.sql.mapper.column;

import lombok.RequiredArgsConstructor;
import org.jooq.DataType;

import java.util.function.Function;

@RequiredArgsConstructor
public
class SimpleColumnMapper<T, V> implements SingleColumnMapper<T, V> {

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
