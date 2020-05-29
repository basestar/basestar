//package io.basestar.jdbc;
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
