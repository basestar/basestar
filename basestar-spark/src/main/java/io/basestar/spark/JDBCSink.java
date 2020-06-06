package io.basestar.spark;

import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class JDBCSink implements Sink<Dataset<?>> {

    private final SparkSession session;

    private final String jdbcUrl;

    private final String table;

    private final SaveMode mode;

    private final Properties properties;

    private final Map<String, String> options;

    @lombok.Builder(builderClassName = "Builder")
    JDBCSink(final SparkSession session, final String jdbcUrl, final String table, final SaveMode mode, final Properties properties, final Map<String, String> options) {

        this.session = Nullsafe.require(session);
        this.jdbcUrl = Nullsafe.require(jdbcUrl);
        this.table = Nullsafe.require(table);
        this.mode = Nullsafe.option(mode, SaveMode.ErrorIfExists);
        this.properties = Nullsafe.option(properties, Properties::new);
        this.options = Nullsafe.option(options);
    }

    @Override
    public void accept(final Dataset<?> input) {

        input.write()
                .options(options)
                .mode(mode)
                .jdbc(jdbcUrl, table, properties);
    }
}
