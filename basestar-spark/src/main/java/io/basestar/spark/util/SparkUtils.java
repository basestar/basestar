package io.basestar.spark.util;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.*;

import java.util.concurrent.Callable;

public class SparkUtils {

    // No-op, but this prevents wrong overload selection

    public static <A, B> MapFunction<A, B> map(final MapFunction<A, B> fn) {

        return fn;
    }

    // No-op, but this prevents wrong overload selection

    public static <A, B> FlatMapFunction<A, B> flatMap(final FlatMapFunction<A, B> fn) {

        return fn;
    }

    // No-op, but this prevents wrong overload selection

    public static <A> FilterFunction<A> filter(final FilterFunction<A> fn) {

        return fn;
    }

    // No-op, but this prevents wrong overload selection

    public static <A, B, C> MapGroupsFunction<A, B, C> mapGroups(final MapGroupsFunction<A, B, C> fn) {

        return fn;
    }

    // No-op, but this prevents wrong overload selection

    public static <A, B, C> FlatMapGroupsFunction<A, B, C> flatMapGroups(final FlatMapGroupsFunction<A, B, C> fn) {

        return fn;
    }

    public static void withJobGroup(final SparkContext sc, final String id, final Runnable t) {

        try {
            sc.setJobGroup(id, id, true);
            t.run();
        } finally {
            sc.clearJobGroup();
        }
    }

    public static <T> T withJobGroup(final SparkContext sc, final String id, final Callable<T> t) throws Exception {

        try {
            sc.setJobGroup(id, id, true);
            return t.call();
        } finally {
            sc.clearJobGroup();
            ;
        }
    }
}
