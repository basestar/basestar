package io.basestar.storage.sql.util;

import lombok.RequiredArgsConstructor;
import org.jooq.DDLQuery;

@RequiredArgsConstructor
public class DDLQueryStep implements DDLStep {

    private final DDLQuery query;

    @Override
    public void execute() {

        query.execute();
    }

    @Override
    public String getSQL() {

        return query.getSQL();
    }
}
