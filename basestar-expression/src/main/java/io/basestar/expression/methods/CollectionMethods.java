package io.basestar.expression.methods;

import com.google.common.collect.Lists;
import io.basestar.expression.function.Lambda;
import io.basestar.expression.type.Values;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public abstract class CollectionMethods<T extends Collection<?>> {

    public int size(final T target) {

        return target.size();
    }

    public boolean isEmpty(final T target) {

        return target.isEmpty();
    }

    public boolean contains(final T target, final Object o) {

        return target.stream().anyMatch(v -> Values.equals(v, o));
    }

    public boolean containsAll(final T target, final Collection<?> os) {

        return os.stream().allMatch(o -> target.stream().anyMatch(v -> Values.equals(v, o)));
    }

    public boolean containsAny(final T target, final Collection<?> os) {

        return os.stream().anyMatch(o -> target.stream().anyMatch(v -> Values.equals(v, o)));
    }

    public boolean anyMatch(final T target, final Lambda.Callable fn) {

        return target.stream().anyMatch(v -> Values.isTruthy(fn.call(v)));
    }

    public boolean allMatch(final T target, final Lambda.Callable fn) {

        return target.stream().anyMatch(v -> Values.isTruthy(fn.call(v)));
    }

    public T map(final T target, final Lambda.Callable fn) {

        return collect(target.stream().map(fn::call));
    }

    public T flatMap(final T target, final Lambda.Callable fn) {

        return collect(target.stream().map(fn::call)
                .map(vs -> {
                    if(vs instanceof Collection<?>) {
                        return ((Collection) vs).stream();
                    } else {
                        throw new IllegalStateException();
                    }
                }));
    }

    @SuppressWarnings("unchecked")
    public Object reduce(final T target, final Lambda.Callable fn) {

        return ((Collection<Object>)target).stream().reduce(fn::call).orElse(null);
    }

    public Object reduce(final T target, final Object initial, final Lambda.Callable fn) {

        return Stream.concat(Stream.of(initial), target.stream())
                .reduce(fn::call).orElseThrow(IllegalStateException::new);
    }

    protected abstract T collect(final Stream<?> stream);


    @SuppressWarnings("unchecked")
    public List<?> sort(final T target) {

        final List<Comparable<Object>> copy = Lists.newArrayList((Collection<Comparable<Object>>)target);
        copy.sort(Comparator.naturalOrder());
        return copy;
    }

    @SuppressWarnings("unchecked")
    public List<?> sort(final T target, final Lambda.Callable fn) {

        final List<Comparable<Object>> copy = Lists.newArrayList((Collection<Comparable<Object>>)target);
        copy.sort((Comparator<Object>) (o1, o2) -> ((Number)fn.call(o1, o2)).intValue());
        return copy;
    }
}
