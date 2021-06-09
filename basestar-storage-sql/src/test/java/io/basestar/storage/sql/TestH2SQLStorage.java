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

import io.basestar.storage.sql.dialect.H2Dialect;
import lombok.extern.slf4j.Slf4j;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;

@Slf4j
class TestH2SQLStorage extends TestSQLStorage {

    @Override
    protected SQLDialect dialect() {

        return new H2Dialect();
    }

    @Override
    protected DataSource dataSource() {

        final JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=100");
        return ds;
    }
}
