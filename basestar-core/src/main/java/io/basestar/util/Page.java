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

import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import lombok.*;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@Data
@With
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Page<T> extends AbstractList<T> implements Serializable {

    public enum Stat {
        TOTAL("total"),
        APPROX_TOTAL("approxTotal", "approx-total");

        public static final char MULTIPLE_DELIMITER = ',';

        private final Set<String> aliases;

        Stat(final String... aliases) {
            this.aliases = new HashSet<>(Arrays.asList(aliases));
            this.aliases.add(name());
        }

        public static Stat parse(final String str) {
            return Arrays.stream(values())
                    .filter(v -> v.aliases.contains(str))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Invalid stat: " + str));
        }

        public static Set<Stat> parseSet(final Iterable<String> strs) {
            return Streams.stream(strs).map(Stat::parse).collect(toSet());
        }

        public static Set<Stat> parseSet(final String str) {
            return parseSet(Splitter.on(MULTIPLE_DELIMITER).omitEmptyStrings().trimResults().split(str));
        }
    }

    @Data
    @With
    public static class Stats implements Serializable {

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

            if (a == null || b == null) {
                return NULL;
            } else {
                final Long total = (a.total == null || b.total == null) ? null : a.total + b.total;
                final Long approxTotal = total != null ? total : ((a.approxTotal == null || b.approxTotal == null) ? null : a.approxTotal + b.approxTotal);
                return new Stats(approxTotal, total);
            }
        }
    }

    private final List<T> items;

    private final Token paging;

    private final Stats stats;

    public Page(final List<T> items, final Token paging) {

        this(items, paging, null);
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
        final List<U> results = delegate.getItems().stream().map(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    public Page<T> filter(final Predicate<? super T> fn) {

        final Page<T> delegate = this;
        final List<T> results = delegate.getItems().stream().filter(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    @Override
    public T get(final int index) {

        return items.get(index);
    }

    @Override
    public int size() {

        return items.size();
    }

    public Page.Envelope<T> toEnvelope() {
        // drop empty stats to avoid empty wrapper in serialized output
        return new Page.Envelope<>(items, paging, Stats.NULL.equals(stats) ? null : stats);
    }

    @Data
    public static class Token implements Serializable {

        private static final BaseEncoding ENCODING = BaseEncoding.base64Url().omitPadding();

        private final byte[] value;

        public static final int MAX_SIZE = 10000;

        public Token(final byte[] value) {

            if (value.length == 0) {
                throw new IllegalStateException("Cannot create empty token");
            } else if (value.length > MAX_SIZE) {
                throw new IllegalStateException("Token is too long (was " + value.length + " bytes)");
            }
            this.value = Arrays.copyOf(value, value.length);
        }

        public Token(final String value) {

            this.value = ENCODING.decode(value);
        }

        @Override
        public String toString() {

            return ENCODING.encode(value);
        }

        public static Token fromStringValue(final String value) {

            return new Token(value.getBytes(StandardCharsets.UTF_8));
        }

        public String getStringValue() {

            return new String(value, StandardCharsets.UTF_8);
        }

        public static Token fromLongValue(final Long value) {

            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(value);
            return new Token(buffer.array());
        }

        public Long getLongValue() {

            final ByteBuffer buffer = ByteBuffer.wrap(getValue());
            return buffer.getLong();
        }
    }

    /**
     * An envelope data structure to wrap Page data in responses.
     * Can be used to avoid special serialization due to Page extending AbstractList.
     *
     * @param <T>
     */
    @Value
    public static class Envelope<T> {
        List<T> items;
        Token paging;
        Stats stats;
    }

}
