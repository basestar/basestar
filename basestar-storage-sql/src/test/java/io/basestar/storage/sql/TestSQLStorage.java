package io.basestar.storage.sql;

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
        final SQLRouting routing = new SQLRouting.Simple(objectSchema, historySchema);

        final List<ObjectSchema> schemas = new ArrayList<>();
        for(final Schema<?> schema : namespace.getSchemas().values()) {
            if(schema instanceof ObjectSchema) {
                schemas.add((ObjectSchema)schema);
            }
        }

        try(final Connection conn = ds.getConnection()) {
            conn.prepareStatement("CREATE DOMAIN IF NOT EXISTS JSONB AS JSON").execute();
            final DSLContext context = DSL.using(conn, SQLDialect.H2);
            routing.createTables(context, schemas);
            conn.commit();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        final Storage storage = SQLStorage.builder()
                .setDataSource(ds)
                .setDialect(SQLDialect.H2)
                .setRouting(routing)
                .build();

        writeAll(storage, namespace, data);

        return storage;
    }
}
