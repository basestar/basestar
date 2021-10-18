package io.basestar.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HadoopFSUtils {

    /**
     * Checks whether the given location does not exist or is an empty directory.
     */
    public static boolean isEmpty(final Configuration configuration, final URI location) {
        try {
            final Path path = new Path(location);
            final FileSystem fs = path.getFileSystem(configuration);
            if (!fs.exists(path)) {
                return true;
            }

            // will return self if it is a file, or contents if it is a directory
            return !fs.listStatusIterator(path).hasNext();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed check if empty location: " + location, e);
        }
    }

}
