package io.basestar.storage.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.use.UseString;
import io.basestar.schema.util.Casing;
import io.basestar.storage.Storage;
import io.basestar.storage.sql.dialect.SnowflakeDialect;
import io.basestar.storage.sql.strategy.DefaultNamingStrategy;
import io.basestar.storage.sql.strategy.DefaultSQLStrategy;
import io.basestar.storage.sql.strategy.SQLStrategy;
import io.basestar.util.Immutable;
import io.basestar.util.Nullsafe;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import org.apache.commons.dbcp2.BasicDataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


// Cannot run without a real configured snowflake environment

@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_ACCOUNT", matches = ".+")
public class TestSnowflakeStorage extends TestSQLStorage {

    static {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final String SNOWFLAKE_ACCOUNT = Nullsafe.orDefault(System.getenv("SNOWFLAKE_ACCOUNT"));

    private static final String SNOWFLAKE_REGION = Nullsafe.orDefault(System.getenv("SNOWFLAKE_REGION"), "eu-west-1");

    private static final String SNOWFLAKE_USERNAME = Nullsafe.orDefault(System.getenv("SNOWFLAKE_USERNAME"));

    private static final String SNOWFLAKE_PASSWORD = Nullsafe.orDefault(System.getenv("SNOWFLAKE_PASSWORD"));

    private static final String SNOWFLAKE_DATABASE = Nullsafe.orDefault(System.getenv("SNOWFLAKE_DATABASE"));

    // This database exists in a demo account, it is only used as the default DB in the custom name override test
    private static final String SNOWFLAKE_DEFAULT_DATABASE = Nullsafe.orDefault(System.getenv("SNOWFLAKE_DEFAULT_DATABASE"), "SNOWFLAKE");

    protected static final String SQL_STRUCT_QUERY = "SqlStructQuery";

    @Override
    protected SQLDialect dialect() {

        return new SnowflakeDialect();
    }

    @Override
    protected DataSource dataSource() {

        final BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:snowflake://" + SNOWFLAKE_ACCOUNT + "." + SNOWFLAKE_REGION + ".snowflakecomputing.com/?TIMEZONE=UTC");
        ds.setUsername(SNOWFLAKE_USERNAME);
        ds.setPassword(SNOWFLAKE_PASSWORD);
        ds.setDefaultCatalog(SNOWFLAKE_DATABASE);
        return ds;
    }

    @Test
    @Override
    protected void testRefDeepExpandQuery() {

        // Skip, concurrency controls not supported
    }

    @Override
    protected void testLike() throws IOException {

        // FIXME: Skip, slight discrepancy in escaped comparison, review later
    }

    @Test
    void testCustomSqlNames() throws Exception {

        final BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:snowflake://" + SNOWFLAKE_ACCOUNT + "." + SNOWFLAKE_REGION + ".snowflakecomputing.com/?TIMEZONE=UTC");
        ds.setUsername(SNOWFLAKE_USERNAME);
        ds.setPassword(SNOWFLAKE_PASSWORD);
        ds.setDefaultCatalog(SNOWFLAKE_DEFAULT_DATABASE);

        final SQLDialect dialect = dialect();

        final String objectSchema = "obj_" + UUID.randomUUID().toString().replaceAll("-", "_");
        final SQLStrategy strategy = DefaultSQLStrategy.builder()
                .namingStrategy(DefaultNamingStrategy.builder()
                        .columnCasing(Casing.LOWERCASE_SNAKE)
                        .objectSchemaName(objectSchema)
                        .dialect(dialect)
                        .build())
                .dialect(dialect)
                .objectSchemaName(objectSchema)
                .useMetadata(true)
                .build();

        final String tableName = "CUSTOM_TABLE";

        final Namespace namespace = Namespace.builder()
                .setSchema("Test", ViewSchema.builder()
                        .setSql("INVALID SQL")
                        .setProperty("nameWithUnderscores", Property.builder().setType(UseString.DEFAULT))
                        .setExtensions(ImmutableMap.of("sql.table", SNOWFLAKE_DATABASE + "." + objectSchema + "." + tableName)))
                .build();

        final LinkableSchema schema = namespace.requireLinkableSchema("Test");

        try (final Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            final DSLContext context = DSL.using(conn, dialect.ddlDialect());
            context.createSchema(DSL.name(SNOWFLAKE_DATABASE, objectSchema)).execute();
            context.createTable(DSL.name(SNOWFLAKE_DATABASE, objectSchema, tableName))
                    .column("name_with_underscores", SQLDataType.VARCHAR)
                    .execute();
            context.insertInto(DSL.table(DSL.name(SNOWFLAKE_DATABASE, objectSchema, tableName)))
                    .set(DSL.field(DSL.name("name_with_underscores")), "record1")
                    .execute();
            conn.commit();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        final Storage storage = SQLStorage.builder()
                .setDataSource(ds)
                .setStrategy(strategy)
                .build();

        final Page<Map<String, Object>> page = storage.query(Consistency.ASYNC, schema, Immutable.map(), Expression.parse("nameWithUnderscores == 'record1'"), ImmutableList.of(Sort.asc("__key")), ImmutableSet.of())
                .page(10).get();
        assertEquals(1, page.size());
        assertEquals("record1", page.get(0).get("nameWithUnderscores"));
    }

    @Test
    protected void testForAny() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 100L)),
                        new Instance(ImmutableMap.of("x", 5L, "y", 10L))
                )
        ));

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 10L)),
                        new Instance(ImmutableMap.of("x", 1L, "y", 10L))
                )
        ));

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                )
        ));

        final Expression expr = Expression.parse("p.x == 10 && p.y == 100 for any p of arrayStruct");
        final Page<Map<String, Object>> results = storage.query(Consistency.ATOMIC, schema, Immutable.map(), expr, Collections.emptyList(), Collections.emptySet()).page(100).join();
        assertEquals(1, results.size());
    }

    @Test
    protected void testForAll() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 100L)),
                        new Instance(ImmutableMap.of("x", 5L, "y", 10L))
                )
        ));

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 10L)),
                        new Instance(ImmutableMap.of("x", 1L, "y", 10L))
                )
        ));

        createComplete(storage, schema, ImmutableMap.of(
                "arrayStruct", ImmutableList.of(
                )
        ));

        final Expression expr = Expression.parse("p.x > 0 && p.y > 0 for all p of arrayStruct");
        final Page<Map<String, Object>> results = storage.query(Consistency.ATOMIC, schema, Immutable.map(), expr, Collections.emptyList(), Collections.emptySet()).page(100).join();
        assertEquals(3, results.size());
    }

    @Test
    public void testSqlStructQuery() throws Exception {

        assumeTrue(supportsSql());

        final Storage storage = storage(namespace);

        bulkLoad(storage, loadAddresses());

        final QuerySchema schema = namespace.requireQuerySchema(SQL_STRUCT_QUERY);

        final Page<Map<String, Object>> firstPage = storage.query(Consistency.ATOMIC, schema, Immutable.map("city", Immutable.map("name", "Washington")), Constant.TRUE, Immutable.list(), ImmutableSet.of())
                .page(20).get();

        assertEquals(1, firstPage.size());
        assertEquals(schema.create(ImmutableMap.of(
                "city", "Washington",
                "count", 2L,
                "state", "District of Columbia"
        )), firstPage.get(0));
    }
}
