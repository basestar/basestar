package io.basestar.util;

/*-
 * #%L
 * basestar-core
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@With
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class PagedList<T> extends AbstractList<T> implements Serializable {

    public enum Stat {

        TOTAL,
        APPROX_TOTAL
    }

    @Data
    @With
    public static class Stats {

        public static final Stats UNKNOWN = new Stats(null, null);

        private final Integer total;

        private final Integer approxTotal;

        public static Stats sum(final Stats a, final Stats b) {

            if(a == null || b == null) {
                return UNKNOWN;
            } else {
                final Integer total = (a.total == null || b.total == null) ? null : a.total + b.total;
                final Integer approxTotal = (a.approxTotal == null || b.approxTotal == null) ? null : a.approxTotal + b.approxTotal;
                return new Stats(total, approxTotal);
            }
        }
    }

    private final List<T> page;

    private final PagingToken paging;

    private final Stats stats;

    public PagedList(final List<T> page, final PagingToken paging) {

        this(page, paging, Stats.UNKNOWN);
    }

    public static <T> PagedList<T> empty() {

        return new PagedList<>(Collections.emptyList(), null, Stats.UNKNOWN);
    }

    public boolean hasPaging() {

        return getPaging() != null;
    }

    public <U> PagedList<U> map(final Function<? super T, ? extends U> fn) {

        final PagedList<T> delegate = this;
        final List<U> results = delegate.getPage().stream().map(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging, stats);
    }

    public PagedList<T> filter(final Predicate<? super T> fn) {

        final PagedList<T> delegate = this;
        final List<T> results = delegate.getPage().stream().filter(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging, stats);
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
