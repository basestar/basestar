package io.basestar.schema.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Bucket implements Serializable {

    private final int[] values;

    public Bucket(final int ... values) {

        this.values = Arrays.copyOf(values, values.length);
    }

    public Bucket(final List<Integer> values) {

        this.values = values.stream().mapToInt(v -> v).toArray();
    }

    public int get(final int id) {

        return values[id];
    }

    public int size() {
        
        return values.length;
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) return true;
        if (!(o instanceof Bucket)) return false;
        final Bucket bucket = (Bucket) o;
        return Arrays.equals(values, bucket.values);
    }

    @Override
    public int hashCode() {

        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {

        return "Bucket[values=" + Arrays.toString(values) + "]";
    }
}
