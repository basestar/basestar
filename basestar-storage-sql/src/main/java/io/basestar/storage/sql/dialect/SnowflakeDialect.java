package io.basestar.storage.sql.dialect;

import org.jooq.DataType;
import org.jooq.impl.SQLDataType;


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
}
