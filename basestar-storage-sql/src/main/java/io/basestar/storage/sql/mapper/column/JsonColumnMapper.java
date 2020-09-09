package io.basestar.storage.sql.mapper.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import lombok.RequiredArgsConstructor;
import org.jooq.DataType;

import java.io.IOException;
import java.util.function.Function;

@RequiredArgsConstructor
public
class JsonColumnMapper<T> implements SingleColumnMapper<T, String> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new BasestarModule());

    private final DataType<String> dataType;

    private final Function<Object, T> marshaller;

    private final Function<T, Object> unmarshaller;

    @Override
    public DataType<String> dataType() {

        return dataType;
    }

    @Override
    public T marshall(final String value) {

        try {
            return value == null ? null : marshaller.apply(objectMapper.readValue(value, Object.class));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String unmarshall(final T value) {

        try {
            return objectMapper.writeValueAsString(value == null ? null : unmarshaller.apply(value));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
