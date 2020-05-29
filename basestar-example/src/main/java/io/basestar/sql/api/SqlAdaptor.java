package io.basestar.sql.api;

import io.basestar.database.Database;
import io.basestar.schema.Namespace;
import io.basestar.util.PagedList;
import lombok.RequiredArgsConstructor;
import org.jooq.Parser;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

@RequiredArgsConstructor
public class SqlAdaptor {

    private final Database database;

    private final Namespace namespace;

    public PagedList<?> sql(final String sql) {

        final Parser parser = DSL.using(SQLDialect.POSTGRES).parser();
        final Query query = parser.parseQuery(sql);
        if(query instanceof SelectQuery<?>) {
            final SelectQuery<?> select = (SelectQuery<?>)query;
            System.err.println(select);
        }
        return null;
    }

    public static void main(final String[] args) {

        new SqlAdaptor(null, null).sql("SELECT * FROM x LEFT OUTER JOIN y USING(a) WHERE a='1' AND b=2");
    }
}
