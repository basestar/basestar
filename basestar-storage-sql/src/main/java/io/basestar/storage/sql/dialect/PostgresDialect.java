package io.basestar.storage.sql.dialect;

public class PostgresDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return org.jooq.SQLDialect.POSTGRES;
    }

    @Override
    public boolean supportsUDFs() {

        return true;
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

    @Override
    public String createFunctionDDLLanguage(final String language) {

        if ("sql".equalsIgnoreCase(language)) {
            return super.createFunctionDDLLanguage("pgplsql");
        } else {
            return super.createFunctionDDLLanguage(language);
        }
    }
}
