package io.basestar.storage.sql.util;

import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;

@RequiredArgsConstructor
public class DDLSqlStep implements DDLStep {

    private final DSLContext context;

    private final String sql;

    @Override
    public void execute() {

        context.execute(sql);
    }

    @Override
    public String getSQL() {

        return sql;
    }
}
