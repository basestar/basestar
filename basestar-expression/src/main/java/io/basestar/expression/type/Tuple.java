package io.basestar.expression.type;

import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@Data
public class Tuple implements Iterable<Object> {

    private final List<Object> values;

    public Tuple(final Collection<?> values) {

        this.values = ImmutableList.copyOf(values);
    }

    public Object get(final int i) {

        return values.get(i);
    }

    @Override
    public Iterator<Object> iterator() {

        return values.iterator();
    }
}
