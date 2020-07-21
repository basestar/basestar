package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
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

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Schema;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import lombok.extern.slf4j.Slf4j;
import org.h2.jdbcx.JdbcDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class TestSQLStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=100");

        final String objectSchema = UUID.randomUUID().toString().replaceAll("-", "_");
        final String historySchema = UUID.randomUUID().toString().replaceAll("-", "_");
        final SQLStrategy strategy = new SQLStrategy.Simple(objectSchema, historySchema);

        final List<ObjectSchema> schemas = new ArrayList<>();
        for(final Schema<?> schema : namespace.getSchemas().values()) {
            if(schema instanceof ObjectSchema) {
                schemas.add((ObjectSchema)schema);
            }
        }

        try(final Connection conn = ds.getConnection()) {
            conn.prepareStatement("CREATE DOMAIN IF NOT EXISTS JSONB AS JSON").execute();
            final DSLContext context = DSL.using(conn, SQLDialect.H2);
            strategy.createTables(context, schemas);
            conn.commit();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        final Storage storage = SQLStorage.builder()
                .setDataSource(ds)
                .setDialect(SQLDialect.H2)
                .setStrategy(strategy)
                .build();

        writeAll(storage, namespace, data);

        return storage;
    }

    protected boolean supportsLike() {

        return true;
    }
}
