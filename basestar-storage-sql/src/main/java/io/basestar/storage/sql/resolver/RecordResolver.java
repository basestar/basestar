package io.basestar.storage.sql.resolver;

import io.basestar.util.Name;
import org.jooq.DataType;

import java.util.Map;

public interface RecordResolver {

    static RecordResolver from(final Map<Name, Object> values) {

        return new RecordResolver() {
            @Override
            public <T> T get(final Name name, final DataType<T> targetType) {

                return targetType.convert(values.get(name));
            }
        };
    }

    <T> T get(Name name, DataType<T> targetType);
}
