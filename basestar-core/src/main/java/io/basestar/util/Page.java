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

import com.google.common.io.BaseEncoding;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@With
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Page<T> extends AbstractList<T> implements Serializable {

    public enum Stat {

        TOTAL,
        APPROX_TOTAL
    }

    @Data
    @With
    public static class Stats {

        public static final Stats ZERO = new Stats(0L, 0L);

        public static final Stats NULL = new Stats(null, null);

        private final Long approxTotal;

        private final Long total;

        public static Stats fromApproxTotal(final long approxTotal) {

            return new Stats(approxTotal, null);
        }

        public static Stats fromTotal(final long total) {

            return new Stats(total, total);
        }

        // Sum is deliberately poisoned by null
        public static Stats sum(final Stats a, final Stats b) {

            if(a == null || b == null) {
                return NULL;
            } else {
                final Long total = (a.total == null || b.total == null) ? null : a.total + b.total;
                final Long approxTotal = total != null ? total : ((a.approxTotal == null || b.approxTotal == null) ? null : a.approxTotal + b.approxTotal);
                return new Stats(approxTotal, total);
            }
        }
    }

    private final List<T> page;

    private final Token paging;

    private final Stats stats;

    public Page(final List<T> page, final Token paging) {

        this(page, paging, null);
    }

    public static <T> Page<T> empty() {

        return new Page<>(Collections.emptyList(), null, null);
    }

    public static <T> Page<T> single(final T value) {

        return new Page<>(Collections.singletonList(value), null, null);
    }

    public static <T> Page<T> from(final List<T> page) {

        return new Page<>(page, null, null);
    }

    public boolean hasMore() {

        return getPaging() != null;
    }

    public <U> Page<U> map(final Function<? super T, ? extends U> fn) {

        final Page<T> delegate = this;
        final List<U> results = delegate.getPage().stream().map(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    public Page<T> filter(final Predicate<? super T> fn) {

        final Page<T> delegate = this;
        final List<T> results = delegate.getPage().stream().filter(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    @Override
    public T get(final int index) {

        return page.get(index);
    }

    @Override
    public int size() {

        return page.size();
    }

    @Data
    public static class Token implements Serializable {

        private static final BaseEncoding ENCODING = BaseEncoding.base64Url().omitPadding();

        private final byte[] value;

        public Token(final byte[] value) {

            this.value = Arrays.copyOf(value, value.length);
        }

        public Token(final String value) {

            this.value = ENCODING.decode(value);
        }

        @Override
        public String toString() {

            return ENCODING.encode(value);
        }
    }
}
