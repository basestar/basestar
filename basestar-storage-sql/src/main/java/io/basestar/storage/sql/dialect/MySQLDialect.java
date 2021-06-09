package io.basestar.storage.sql.dialect;

import org.jooq.SQLDialect;

public class MySQLDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return SQLDialect.MYSQL;
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
