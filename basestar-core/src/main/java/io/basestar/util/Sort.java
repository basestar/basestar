package io.basestar.util;

import com.google.common.base.Splitter;
import lombok.Data;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

@Data
public class Sort implements Serializable {

    public static final String DELIMITER = ":";

    private static final Splitter SPLITTER = Splitter.on(DELIMITER).trimResults().omitEmptyStrings().limit(2);

    private final Path path;

    private final Order order;

    public Sort reverse() {

        return new Sort(path, order.reverse());
    }

    public static Sort asc(final Path path) {

        return new Sort(path, Order.ASC);
    }

    public static Sort desc(final Path path) {

        return new Sort(path, Order.DESC);
    }

//    @JsonCreator
    public static Sort parse(final String v) {

        final List<String> parts = SPLITTER.splitToList(v);
        if(parts.size() < 1) {
            throw new IllegalStateException();
        } else {
            final Path path = Path.parse(parts.get(0));
            final Order order;
            if (parts.size() == 2) {
                order = Order.valueOf(parts.get(1).toUpperCase());
            } else {
                order = Order.ASC;
            }
            return new Sort(path, order);
        }
    }

    public <T, V extends Comparable<V>> Comparator<T> comparator(final BiFunction<T, Path, V> getter) {

        final Comparator<V> cmp = (order == Order.DESC) ? Comparator.reverseOrder() : Comparator.naturalOrder();
        return (a, b) -> {
            final V va = getter.apply(a, path);
            final V vb = getter.apply(b, path);
            return Objects.compare(va, vb, cmp);
        };
    }

    public static <T, V extends Comparable<V>> Comparator<T> comparator(final Collection<Sort> sort, BiFunction<T, Path, V> getter) {

        Comparator<T> result = null;
        for(final Sort s : sort) {
            if(result == null) {
                result = s.comparator(getter);
            } else {
                result = result.thenComparing(s.comparator(getter));
            }
        }
        return result;
    }

    @Override
//    @JsonValue
    public String toString() {

        return path + DELIMITER + order;
    }

    public enum Order {

        ASC,
        DESC;

        public Order reverse() {

            return this == ASC ? DESC : ASC;
        }
    }
}
