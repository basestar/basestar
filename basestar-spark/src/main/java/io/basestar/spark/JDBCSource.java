package io.basestar.spark;

import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class JDBCSource implements Source<Dataset<Row>> {

    private final SparkSession session;

    private final String jdbcUrl;

    private final String table;

    private final Properties properties;

    private final Map<String, String> options;

    @lombok.Builder(builderClassName = "Builder")
    JDBCSource(final SparkSession session, final String jdbcUrl, final String table, final Properties properties, final Map<String, String> options) {

        this.session = Nullsafe.require(session);
        this.jdbcUrl = Nullsafe.require(jdbcUrl);
        this.table = Nullsafe.require(table);
        this.properties = Nullsafe.option(properties, Properties::new);
        this.options = Nullsafe.option(options);
    }

    @Override
    public void then(final Sink<Dataset<Row>> sink) {

        final Dataset<Row> dataset = session.read()
                .options(options)
                .jdbc(jdbcUrl, table, properties);
        log.info("Loaded {} {}", jdbcUrl, table);
        sink.accept(dataset);
    }
}
