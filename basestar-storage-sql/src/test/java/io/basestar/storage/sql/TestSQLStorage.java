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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

abstract class TestSQLStorage extends TestStorage {

    protected boolean supportsLike() {

        return true;
    }

    @Override
    protected void testMultiValueIndex() {

    }

    @Override
    protected void testVersionedRef() {

        // FIXME: versioned ref storage will need a strategy that supports multiple output columns per input value
    }

    @Override
    protected Storage storage(final Namespace namespace) {

        final DataSource ds = dataSource();
        final SQLDialect dialect = dialect();

        final String objectSchema = "obj_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final String historySchema = "his_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final SQLStrategy strategy = SQLStrategy.Simple.builder()
                .objectSchemaName(objectSchema)
                .historySchemaName(historySchema)
                .useMetadata(useMetadata())
                .dialect(dialect)
                .build();

        try(final Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            final DSLContext context = DSL.using(conn, dialect.ddlDialect());
            strategy.createEntities(context, namespace.getSchemas().values());
            conn.commit();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        return SQLStorage.builder()
                .setDataSource(ds)
                .setStrategy(strategy)
                .build();
    }

    protected boolean useMetadata() {

        return true;
    }

    protected abstract SQLDialect dialect();

    protected abstract DataSource dataSource();

    @Test
    public void testSpecialChars() throws Exception {

        final Storage storage = storage(namespace);
        final ObjectSchema schema = namespace.requireObjectSchema(ADDRESS);

        storage.query(Consistency.ASYNC, schema, Expression.parse("id == 'SOME:ID'"), ImmutableList.of(), ImmutableSet.of())
                .page(1).get();
    }

    @Override
    protected boolean supportsAggregation() {

        return true;
    }

    @Override
    protected boolean supportsMaterializedView() {

        return true;
    }

    @Override
    protected boolean supportsSql() {

        return true;
    }
}
