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
//
//import java.sql.*;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.Executor;
//
//public class BasestarConnection implements Connection {
//
//    private final Database database;
//
//    public BasestarConnection(final Database database) {
//
//        this.database = database;
//    }
//
//    @Override
//    public Statement createStatement() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public CallableStatement prepareCall(final String sql) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public String nativeSQL(final String sql) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void setAutoCommit(final boolean autoCommit) throws SQLException {
//
//    }
//
//    @Override
//    public boolean getAutoCommit() throws SQLException {
//        return false;
//    }
//
//    @Override
//    public void commit() throws SQLException {
//
//    }
//
//    @Override
//    public void rollback() throws SQLException {
//
//    }
//
//    @Override
//    public void close() throws SQLException {
//
//    }
//
//    @Override
//    public boolean isClosed() throws SQLException {
//        return false;
//    }
//
//    @Override
//    public DatabaseMetaData getMetaData() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void setReadOnly(final boolean readOnly) throws SQLException {
//
//    }
//
//    @Override
//    public boolean isReadOnly() throws SQLException {
//        return false;
//    }
//
//    @Override
//    public void setCatalog(final String catalog) throws SQLException {
//
//    }
//
//    @Override
//    public String getCatalog() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void setTransactionIsolation(final int level) throws SQLException {
//
//    }
//
//    @Override
//    public int getTransactionIsolation() throws SQLException {
//        return 0;
//    }
//
//    @Override
//    public SQLWarning getWarnings() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void clearWarnings() throws SQLException {
//
//    }
//
//    @Override
//    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Map<String, Class<?>> getTypeMap() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
//
//    }
//
//    @Override
//    public void setHoldability(final int holdability) throws SQLException {
//
//    }
//
//    @Override
//    public int getHoldability() throws SQLException {
//        return 0;
//    }
//
//    @Override
//    public Savepoint setSavepoint() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Savepoint setSavepoint(final String name) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void rollback(final Savepoint savepoint) throws SQLException {
//
//    }
//
//    @Override
//    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
//
//    }
//
//    @Override
//    public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Clob createClob() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Blob createBlob() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public NClob createNClob() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public SQLXML createSQLXML() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public boolean isValid(final int timeout) throws SQLException {
//        return false;
//    }
//
//    @Override
//    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
//
//    }
//
//    @Override
//    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
//
//    }
//
//    @Override
//    public String getClientInfo(final String name) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Properties getClientInfo() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void setSchema(final String schema) throws SQLException {
//
//    }
//
//    @Override
//    public String getSchema() throws SQLException {
//        return null;
//    }
//
//    @Override
//    public void abort(final Executor executor) throws SQLException {
//
//    }
//
//    @Override
//    public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {
//
//    }
//
//    @Override
//    public int getNetworkTimeout() throws SQLException {
//        return 0;
//    }
//
//    @Override
//    public <T> T unwrap(final Class<T> iface) throws SQLException {
//        return null;
//    }
//
//    @Override
//    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
//        return false;
//    }
//}
