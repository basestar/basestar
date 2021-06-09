package io.basestar.storage.sql.dialect;

public class PostgresDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return org.jooq.SQLDialect.POSTGRES;
    }

    @Override
    public boolean supportsConstraints() {

        return true;
    }

    @Override
    public boolean supportsIndexes() {

        return true;
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
