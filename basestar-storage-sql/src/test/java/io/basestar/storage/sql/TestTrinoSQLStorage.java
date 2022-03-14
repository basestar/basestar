package io.basestar.storage.sql;

import io.basestar.storage.sql.dialect.TrinoDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.TrinoContainer;

import javax.sql.DataSource;

@Slf4j
@Disabled
public class TestTrinoSQLStorage extends TestSQLStorage {

    private static final String USERNAME = "test";

    private static final TrinoContainer trino;;

    static {
        try {
            trino = new TrinoContainer("trinodb/trino")
                    .withUsername(USERNAME);
            trino.start();
            Thread.sleep(10000);
        } catch (final Exception e) {
            log.error("Failed to start Trino container", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected io.basestar.storage.sql.SQLDialect dialect() {

        return new TrinoDialect();
    }

    @Override
    protected DataSource dataSource() {

        final BasicDataSource ds = new BasicDataSource();
        ds.setUrl(trino.getJdbcUrl());
        ds.setUsername(USERNAME);
        ds.setDefaultCatalog("memory");
        return ds;
    }

    @Override
    protected void testNullBeforeDelete() {

        // Trino connector does not support deletes
    }

    @Override
    protected void testDeleteMissing() {

        // Trino connector does not support deletes
    }

    @Override
    protected void testDelete() {

        // Trino connector does not support deletes
    }

    @Override
    protected void testPolymorphicDelete() {

        // Trino connector does not support deletes
    }

    @Override
    protected void testDeleteWrongVersion() {

        // Trino connector does not support deletes
    }

    @Override
    protected void testNullBeforeUpdate() {

        // Trino connector does not support updates
    }

    @Override
    protected void testUpdateMissing() {

        // Trino connector does not support updates
    }

    @Override
    protected void testUpdate() {

        // Trino connector does not support updates
    }

    @Override
    protected void testUpdateWrongVersion() {

        // Trino connector does not support updates
    }

    @Override
    protected void testPolymorphicCreate() {

        // Trino connector does not support constraints
    }

    @Override
    protected void testCreateConflict() {

        // Trino connector does not support constraints
    }

    @Override
    protected void testLarge() {

        // Not a use case at the moment
    }

    @Override
    protected void testDateSort() {

        // FIXME
    }

    @Override
    protected void testLike() {

        // FIXME
    }

    @Override
    protected void testCreate() {

        // Trino connector does not support create
    }

    @Override
    protected void testRefIndex() {

        // FIXME
    }

    @Override
    protected boolean supportsMaterializedView() {

        // Trino connector does not support write
        return false;
    }

    @Override
    protected boolean supportsSql() {

        return false;
    }
}
