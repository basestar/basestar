package io.basestar.spark;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.List;

@RequiredArgsConstructor
public class CriteriaSink<T, C> implements Sink<RDD<T>> {

    private final Criteria<T, C> criteria;

    private final Selector<T, C> selector;

    @Override
    public void accept(final RDD<T> input) {

        final JavaRDD<T> rdd = input.toJavaRDD().cache();
        final List<C> all = rdd.map(criteria::apply).distinct().collect();

        for (final C c : all) {
            selector.apply(c).accept(rdd.filter(v -> c.equals(criteria.apply(v))).rdd());
        }
    }

    public interface Criteria<T, C> extends Serializable {

        C apply(T value);
    }

    public interface Selector<T, C> extends Serializable {

        Sink<RDD<T>> apply(C criteria);
    }
}
