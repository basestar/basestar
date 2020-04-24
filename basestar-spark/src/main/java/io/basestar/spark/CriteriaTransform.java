package io.basestar.spark;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CriteriaTransform<T extends Serializable, C extends Serializable> implements Transform<RDD<T>, Map<C, RDD<T>>> {

    private final Criteria<T, C> criteria;

    @Override
    public Map<C, RDD<T>> accept(final RDD<T> input) {

        final JavaRDD<T> rdd = input.toJavaRDD().cache();
        final List<C> all = rdd.map(criteria::apply).distinct().collect();

        return all.stream()
                .collect(Collectors.toMap(
                        c -> c,
                        c -> rdd.filter(v -> c.equals(criteria.apply(v))).rdd()
                ));
    }

    public interface Criteria<T, C> extends Serializable {

        C apply(T value);
    }
}
