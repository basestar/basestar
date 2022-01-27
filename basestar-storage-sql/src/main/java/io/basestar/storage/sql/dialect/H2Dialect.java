package io.basestar.storage.sql.dialect;

public class H2Dialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return org.jooq.SQLDialect.H2;
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

        return true;
    }
}
