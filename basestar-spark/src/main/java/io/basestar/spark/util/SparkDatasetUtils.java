package io.basestar.spark.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SparkDatasetUtils {

    public static final String PARTITIONS = "io.basestar.partitions";

    public static <T> Dataset<T> addPartitionSpec(final Dataset<T> ds, final Collection<Map<String, String>> spec) {

        return ds.hint(PARTITIONS, copy(spec));
    }

    private static List<Map<String, String>> copy(final Collection<Map<String, String>> spec) {

        final ImmutableList.Builder<Map<String, String>> builder = ImmutableList.builder();
        spec.forEach(s -> builder.add(ImmutableMap.copyOf(s)));
        return builder.build();
    }
}
