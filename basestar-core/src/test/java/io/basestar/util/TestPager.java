package io.basestar.util;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class TestPager {

    private final Page.Token page1 = new Page.Token(new byte[]{1});

    Page<String> source1(final Page.Token token) {

        if (token == null) {
            return new Page<>(ImmutableList.of("a", "c"), page1, Page.Stats.fromTotal(10));
        } else if (token.equals(page1)) {
            return new Page<>(ImmutableList.of("e", "g"), null, Page.Stats.fromTotal(8));
        } else {
            return Page.empty();
        }
    }

    Page<String> source2(final Page.Token token) {

        if (token == null) {
            return new Page<>(ImmutableList.of("b", "d"), page1, Page.Stats.fromTotal(10));
        } else if (token.equals(page1)) {
            return new Page<>(ImmutableList.of("f", "h"), null, Page.Stats.fromTotal(8));
        } else {
            return Page.empty();
        }
    }

    @Test
    void testStats() throws ExecutionException, InterruptedException {

        final Pager<String> a = (count, token, stats) -> CompletableFuture.completedFuture(source1(token));
        final Pager<String> b = (count, token, stats) -> CompletableFuture.completedFuture(source2(token));

        final Pager<String> pager = Pager.merge(String::compareTo, ImmutableMap.of("a", a, "b", b));

        final Page<String> page = pager.page(EnumSet.of(Page.Stat.TOTAL), null, 10).get();
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h"), page.getItems());
        assertNull(page.getPaging());
        assertNotNull(page.getStats());
        assertEquals(20, page.getStats().getTotal());
        assertEquals(20, page.getStats().getApproxTotal());
    }


    private Pager<Integer> sort() {

        return Pager.merge(Comparator.naturalOrder(), ImmutableMap.of(
                "a", (stats, token, count) -> CompletableFuture.supplyAsync(() ->
                        token == null
                                ? new Page<>(ImmutableList.of(1, 3, 5), new Page.Token("1".getBytes(Charsets.UTF_8)))
                                : new Page<>(ImmutableList.of(8, 10, 12), null)
                ),
                "b", (stats, token, count) -> CompletableFuture.supplyAsync(() ->
                        token == null
                                ? new Page<>(ImmutableList.of(2, 3, 4, 6, 6), new Page.Token("1".getBytes(Charsets.UTF_8)))
                                : new Page<>(ImmutableList.of(7, 7, 7, 9, 11, 12, 13, 14, 15), null)
                )
        ));
    }

    @Test
    void testPaging() {

        final Page<Integer> page1 = sort().page(3).join();
        assertEquals(Lists.newArrayList(1, 2, 3), page1.getItems());
        assertNotNull(page1.getPaging());
        final Page<Integer> page2 = sort().page(page1.getPaging(), 2).join();
        assertEquals(Lists.newArrayList(4, 5), page2.getItems());
        assertNotNull(page1.getPaging());
        final Page<Integer> page3 = sort().page(page2.getPaging(), 1).join();
        assertEquals(Lists.newArrayList(6), page3.getItems());
        assertNotNull(page3.getPaging());
        final Page<Integer> page4 = sort().page(page3.getPaging(), 6).join();
        assertEquals(Lists.newArrayList(7, 8, 9, 10, 11, 12), page4.getItems());
        assertNotNull(page4.getPaging());
        final Page<Integer> page5 = sort().page(page4.getPaging(), 10).join();
        assertEquals(Lists.newArrayList(13, 14, 15), page5.getItems());
        assertNull(page5.getPaging());
    }
}
