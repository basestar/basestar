package io.basestar.spark;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.Map;

@Data
@RequiredArgsConstructor
public class GenericSource implements Source<Dataset<Row>> {

    public static final String DEFAULT_FORMAT = Format.PARQUET;

    private final SparkSession session;

    private final String format;

    private final String path;

    private final Map<String, String> options;

    public GenericSource(final SparkSession session, final String path) {

        this(session, DEFAULT_FORMAT, path);
    }

    public GenericSource(final SparkSession session, final String format, final String path) {

        this(session, format, path, Collections.emptyMap());
    }

    @Override
    public void sink(final Sink<Dataset<Row>> sink) {

        sink.accept(session.read()
                .options(options)
                .format(format)
                .load(path));
    }
}
