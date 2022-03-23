package io.basestar.storage.sql.resolver;

import io.basestar.expression.exception.UndefinedNameException;
import io.basestar.util.Name;
import org.jooq.Field;

import java.util.Optional;

public interface ColumnResolver {

    Optional<Field<?>> column(Name name);

    default Field<?> requireColumn(final Name name) {

        return column(name).orElseThrow(() -> new UndefinedNameException(name.toString()));
    }
}
