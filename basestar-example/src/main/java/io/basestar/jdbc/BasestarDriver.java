//package io.basestar.jdbc;

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
//
//import io.basestar.database.Database;
//import lombok.extern.slf4j.Slf4j;
//
//import java.sql.*;
//import java.util.Properties;
//import java.util.logging.Logger;
//
//@Slf4j
//public class BasestarDriver implements Driver {
//
//    static
//    {
//        try {
//            final BasestarDriver driver = new BasestarDriver();
//            DriverManager.registerDriver(driver);
//        } catch (final SQLException e) {
//            log.error("Failed to register basestar JDBC driver", e);
//        }
//    }
//
//    @Override
//    public Connection connect(final String url, final Properties info) throws SQLException {
//
//        final Database database = new DatabaseClient();
//        return new BasestarConnection(database);
//    }
//
//    @Override
//    public boolean acceptsURL(final String url) throws SQLException {
//
//        return false;
//    }
//
//    @Override
//    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
//
//        return new DriverPropertyInfo[0];
//    }
//
//    @Override
//    public int getMajorVersion() {
//
//        return 0;
//    }
//
//    @Override
//    public int getMinorVersion() {
//
//        return 0;
//    }
//
//    @Override
//    public boolean jdbcCompliant() {
//
//        return false;
//    }
//
//    @Override
//    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
//
//        return null;
//    }
//}
