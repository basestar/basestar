package io.basestar.util;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Wither;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@Wither
@EqualsAndHashCode(callSuper = false)
public class PagedList<T> extends AbstractList<T> implements Serializable {

    private final List<T> page;

    private final PagingToken paging;

    public static <T> PagedList<T> empty() {

        return new PagedList<>(Collections.emptyList(), null);
    }

    public boolean hasPaging() {

        return getPaging() != null;
    }

    public <U> PagedList<U> map(final Function<? super T, ? extends U> fn) {

        final PagedList<T> delegate = this;
        final List<U> results = delegate.getPage().stream().map(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging);
    }

    public PagedList<T> filter(final Predicate<? super T> fn) {

        final PagedList<T> delegate = this;
        final List<T> results = delegate.getPage().stream().filter(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging);
    }

    @Override
    public T get(final int index) {

        return page.get(index);
    }

    @Override
    public int size() {

        return page.size();
    }
}
