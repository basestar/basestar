//package io.basestar.storage.sql;
//
//import com.mysql.cj.jdbc.MysqlDataSource;
//import io.basestar.storage.sql.dialect.MySQLDialect;
//import lombok.extern.slf4j.Slf4j;
//import org.testcontainers.containers.JdbcDatabaseContainer;
//import org.testcontainers.containers.MySQLContainerProvider;
//
//import javax.sql.DataSource;
//import java.sql.SQLException;
//
//@Slf4j
//public class TestMySQLStorage extends TestSQLStorage {
//
//    private static final String USERNAME = "test";
//
//    private static final String PASSWORD = "test";
//
//    private static final JdbcDatabaseContainer<?> mysql;;
//
//    static {
//        try {
//            mysql = new MySQLContainerProvider().newInstance()
//                    .withUsername(USERNAME)
//                    .withPassword(PASSWORD);
//            mysql.start();
//            Thread.sleep(10000);
//        } catch (final Exception e) {
//            log.error("Failed to start MySQL container", e);
//            throw new IllegalStateException(e);
//        }
//    }
//
//    @Override
//    protected SQLDialect dialect() {
//
//        return new MySQLDialect();
//    }
//
//    @Override
//    protected DataSource dataSource() {
//
//            final MysqlDataSource ds = new MysqlDataSource();
//            ds.setUrl(mysql.getJdbcUrl());
//            ds.setUser(USERNAME);
//            ds.setPassword(PASSWORD);
//            return ds;
//
//
//    }
//}
