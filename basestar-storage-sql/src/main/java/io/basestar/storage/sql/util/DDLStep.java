package io.basestar.storage.sql.util;

import org.jooq.DDLQuery;
import org.jooq.DSLContext;

public interface DDLStep {

    static DDLStep from(final DDLQuery query) {

        return new DDLQueryStep(query);
    }

    static DDLStep from(final DSLContext context, final String sql) {

        return new DDLSqlStep(context, sql);
    }

    void execute();

    String getSQL();
}
