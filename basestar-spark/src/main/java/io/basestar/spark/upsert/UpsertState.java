package io.basestar.spark.upsert;

import io.basestar.util.Text;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.function.Function;
import java.util.function.Supplier;

public interface UpsertState {

    <T> T getState(SparkSession session, String key, Class<T> as);

    <T> void setState(SparkSession session, String key, T value);

    void dropState(SparkSession session, String key);

    default boolean hasState(final SparkSession session, final String key) {

        return getState(session, key, Object.class) != null;
    }

    default <T> T updateState(final SparkSession session, final String key, final Class<T> as, final Function<T, T> apply) {

        final T before = getState(session, key, as);
        final T after = apply.apply(before);
        setState(session, key, after);
        return after;
    }

    default <T> T provisionState(final SparkSession session, final String key, final Class<T> as, final Supplier<T> supplier) {

        final T before = getState(session, key, as);
        if(before == null) {
            final T after = supplier.get();
            setState(session, key, after);
            return after;
        } else {
            return before;
        }
    }

    @Data
    class Hdfs implements UpsertState {

        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final URI location;

        private Path keyPath(final String key) {

            return new Path(Text.ensureSuffix(location.toString(), "/") + key + ".json");
        }

        @Override
        public <T> T getState(final SparkSession session, final String key, final Class<T> as) {

            try {
                final Configuration configuration = session.sparkContext().hadoopConfiguration();
                final Path path = keyPath(key);
                final FileSystem fs = path.getFileSystem(configuration);
                if(fs.exists(path)) {
                    try(final InputStream is = fs.open(path)) {
                        return OBJECT_MAPPER.readValue(is, as);
                    }
                } else {
                    return null;
                }
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to read state " + key, e);
            }
        }

        @Override
        public <T> void setState(final SparkSession session, final String key, final T value) {

            try {
                final Configuration configuration = session.sparkContext().hadoopConfiguration();
                final Path path = keyPath(key);
                final FileSystem fs = path.getFileSystem(configuration);
                try(final OutputStream os = fs.create(path, true)) {
                    OBJECT_MAPPER.writeValue(os, value);
                }
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to set state" + key, e);
            }
        }

        @Override
        public void dropState(SparkSession session, String key) {

            try {
                final Configuration configuration = session.sparkContext().hadoopConfiguration();
                final Path path = keyPath(key);
                final FileSystem fs = path.getFileSystem(configuration);
                if(fs.exists(path)) {
                    fs.delete(path, true);
                }
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to delete state" + key, e);
            }
        }
    }
}
