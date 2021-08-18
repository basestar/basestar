package io.basestar.storage.sql.dialect;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ViewSchema;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.Optional;


public class SnowflakeDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return org.jooq.SQLDialect.POSTGRES;
    }

    @Override
    protected DataType<?> jsonType() {

        return SQLDataType.LONGVARCHAR;
    }

    @Override
    protected Object castJson(final String value) {

        return value;
    }

    @Override
    public boolean supportsConstraints() {

        return false;
    }

    @Override
    public boolean supportsIndexes() {

        return false;
    }

    @Override
    public boolean supportsILike() {

        return true;
    }

    @Override
    protected boolean isJsonEscaped() {

        return false;
    }

    @Override
    public Optional<? extends Field<?>> missingMetadataValue(final LinkableSchema schema, final String name) {

        if (schema instanceof ViewSchema && ViewSchema.ID.equals(name)) {
            return Optional.of(DSL.inline((String) null));
        } else {
            return Optional.empty();
        }
    }
}
