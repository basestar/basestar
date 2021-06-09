package io.basestar.storage.sql;

import io.basestar.storage.sql.dialect.PostgresDialect;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainerProvider;

import javax.sql.DataSource;

@Slf4j
@Disabled
public class TestPostgreSQLStorage extends TestSQLStorage {

    private static final String USERNAME = "test";

    private static final String PASSWORD = "test";

    private static final JdbcDatabaseContainer<?> postgres;;

    static {
        try {
            postgres = new PostgreSQLContainerProvider().newInstance()
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD);
            postgres.start();
            Thread.sleep(10000);
        } catch (final Exception e) {
            log.error("Failed to start Postgres container", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected SQLDialect dialect() {

        return new PostgresDialect();
    }

    @Override
    protected DataSource dataSource() {

        final PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(postgres.getJdbcUrl());
        ds.setUser(USERNAME);
        ds.setPassword(PASSWORD);
        return ds;
    }
}
