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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
public class Sort implements Serializable {

    public static final char DELIMITER = ':';

    public static final char ALT_DELIMITER = ' ';

    public static final char MULTIPLE_DELIMITER = ',';

    private static final CharMatcher DELIMITER_MATCHER = CharMatcher.is(DELIMITER).or(CharMatcher.is(ALT_DELIMITER));

    private static final Splitter SPLITTER = Splitter.on(DELIMITER_MATCHER).trimResults().omitEmptyStrings().limit(2);

    private final Name name;

    private final Order order;

    private final Nulls nulls;

    public Sort(final Name name, final Order order) {

        this(name, order, Nulls.FIRST);
    }

    public static List<Sort> parseList(final Iterable<String> strs) {

        return Streams.stream(strs).map(Sort::parse)
                .collect(Collectors.toList());
    }

    public static List<Sort> parseList(final String ... strs) {

        return parseList(Arrays.asList(strs));
    }

    public static List<Sort> parseList(final String str) {

        return parseList(Splitter.on(MULTIPLE_DELIMITER).omitEmptyStrings().trimResults().split(str));
    }

    public Sort reverse() {

        return new Sort(name, order.reverse(), nulls.reverse());
    }

    public static Sort asc(final String name) {

        return asc(Name.parseNonEmpty(name));
    }

    public static Sort asc(final Name name) {

        return new Sort(name, Order.ASC);
    }

    public static Sort desc(final String name) {

        return desc(Name.parseNonEmpty(name));
    }

    public static Sort desc(final Name name) {

        return new Sort(name, Order.DESC);
    }

//    @JsonCreator
    public static Sort parse(final String v) {

        final List<String> parts = Lists.newArrayList(SPLITTER.split(v));
        if(parts.isEmpty()) {
            throw new IllegalStateException();
        } else {
            final Name name = Name.parse(parts.get(0));
            final Order order;
            if (parts.size() == 2) {
                order = Order.valueOf(parts.get(1).toUpperCase());
            } else {
                order = Order.ASC;
            }
            return new Sort(name, order);
        }
    }

    public <T, V extends Comparable<V>> Comparator<T> comparator(final BiFunction<T, Name, V> getter) {

        final Comparator<V> cmp = (order == Order.DESC) ? Comparator.reverseOrder() : Comparator.naturalOrder();
        return (a, b) -> {
            final V va = getter.apply(a, name);
            final V vb = getter.apply(b, name);
            return Objects.compare(va, vb, cmp);
        };
    }

    public static <T, V extends Comparable<V>> Comparator<T> comparator(final Collection<Sort> sort, final BiFunction<T, Name, V> getter) {

        Comparator<T> result = null;
        for(final Sort s : sort) {
            if(result == null) {
                result = s.comparator(getter);
            } else {
                result = result.thenComparing(s.comparator(getter));
            }
        }
        if(result == null) {
            // Need to return something
            return Comparator.comparing(System::identityHashCode);
        } else {
            return result;
        }
    }

    @Override
//    @JsonValue
    public String toString() {

        return name + String.valueOf(DELIMITER) + order;
    }

    public enum Order {

        ASC,
        DESC;

        public Order reverse() {

            return this == ASC ? DESC : ASC;
        }
    }

    public enum Nulls {

        FIRST,
        LAST;

        public Nulls reverse() {

            return this == FIRST ? LAST : FIRST;
        }
    }
}
