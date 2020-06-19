package io.basestar.sql.api;

/*-
 * #%L
 * basestar-example
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
