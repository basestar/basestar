package io.basestar.storage.sql;

import io.basestar.schema.util.Casing;
import io.basestar.storage.sql.strategy.DefaultNamingStrategy;
import io.basestar.storage.sql.strategy.NamingStrategy;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestH2SQLStorageCased extends TestH2SQLStorage {

    @Override
    protected boolean supportsSql() {

        // SQL in schemas will not work because column names are hardcoded
        return false;
    }

    @Test
    @Override
    protected void testAggregation() throws IOException {

        // skipped, not required in a case-transformed context at present
    }

    @Override
    protected NamingStrategy namingStrategy(final SQLDialect dialect, final String objectSchema, final String historySchema) {

        return DefaultNamingStrategy.builder()
                .columnCasing(Casing.UPPERCASE_SNAKE)
                .entityCasing(Casing.UPPERCASE_SNAKE)
                .objectSchemaName(objectSchema)
                .historySchemaName(historySchema)
                .dialect(dialect)
                .build();
    }
}
