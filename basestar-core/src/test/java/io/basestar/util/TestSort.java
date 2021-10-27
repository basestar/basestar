package io.basestar.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestSort {

    @Test
    void testSortParse() {

        assertEquals(Sort.asc("a"), Sort.parse("a:asc"));
        assertEquals(Sort.asc("a"), Sort.parse("a"));
        assertEquals(Sort.asc("a"), Sort.parse(" a "));
        assertEquals(Sort.desc("b"), Sort.parse("b desc"));
        assertEquals(Sort.desc("b"), Sort.parse(" b  desc"));
        assertEquals(Sort.desc("b"), Sort.parse(" b : desc"));
        assertEquals(ImmutableList.of(Sort.asc("a"), Sort.desc("b")), Sort.parseList("a:asc, b desc"));
        assertEquals(ImmutableList.of(Sort.asc("a"), Sort.desc("b")), Sort.parseList("a:asc", "b desc"));
        assertEquals("a:DESC", Sort.desc("a").toString());
    }

    @Test
    void testSortReverse() {

        assertEquals(new Sort(Name.of("a"), Sort.Order.ASC, Sort.Nulls.FIRST),
                new Sort(Name.of("a"), Sort.Order.DESC, Sort.Nulls.LAST).reverse());
    }

    @Test
    void testSortComparator() {

        final Comparator<Map<String, String>> cmp = Sort.comparator(
                Sort.parseList("a:asc,b:desc"),
                (v, k) -> v.get(k.first())
        );
        final Map<String, String> v1 = ImmutableMap.of("a", "1", "b", "7");
        final Map<String, String> v2 = ImmutableMap.of("a", "2", "b", "6");
        final Map<String, String> v3 = new HashMap<String, String>() {{
            put("a", "3");
            put("b", null);
        }};
        final Map<String, String> v4 = ImmutableMap.of("a", "3", "b", "5");
        final Map<String, String> v5 = ImmutableMap.of("a", "3", "b", "4");
        final Map<String, String> v6 = ImmutableMap.of("a", "3", "b", "3");
        final Map<String, String> v7 = ImmutableMap.of("a", "3", "b", "2");
        final Map<String, String> v8 = ImmutableMap.of("a", "3", "b", "1");

        final Map<String, String> v9 = new HashMap<String, String>() {{
            put("a", null);
            put("b", null);
        }};
        final Map<String, String> v10 = new HashMap<String, String>() {{
            put("a", null);
            put("b", "1");
        }};


        final List<Map<String, String>> expected = ImmutableList.of(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
        final List<Map<String, String>> unsorted = ImmutableList.of(v7, v3, v1, v6, v4, v5, v2, v8, v10, v9);
        final List<Map<String, String>> sorted = unsorted.stream().sorted(cmp).collect(Collectors.toList());
        assertEquals(expected, sorted);
    }
}
