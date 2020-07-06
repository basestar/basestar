package io.basestar.storage.util;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class TestPager {

    private Pager<Integer> sort(final PagingToken paging) {

        return new Pager<Integer>(Comparator.naturalOrder(), ImmutableList.of(
                (count, token, stats) -> CompletableFuture.supplyAsync(() ->
                        token == null
                                ? new PagedList<>(ImmutableList.of(1, 3, 5), new PagingToken("1".getBytes(Charsets.UTF_8)))
                                : new PagedList<>(ImmutableList.of(8, 10, 12), null)
                ),
                (count, token, stats) -> CompletableFuture.supplyAsync(() ->
                        token == null
                                ? new PagedList<>(ImmutableList.of(2, 3, 4, 6, 6), new PagingToken("1".getBytes(Charsets.UTF_8)))
                                : new PagedList<>(ImmutableList.of(7, 7, 7, 9, 11, 12, 13, 14, 15), null)
                )
        ), paging);
    }

    @Test
    public void testPager() {

        final PagedList<Integer> page1 = sort(null).page(3).join();
        assertEquals(Lists.newArrayList(1, 2, 3), page1.getPage());
        assertNotNull(page1.getPaging());
        final PagedList<Integer> page2 = sort(page1.getPaging()).page(2).join();
        assertEquals(Lists.newArrayList(4, 5), page2.getPage());
        assertNotNull(page1.getPaging());
        final PagedList<Integer> page3 = sort(page2.getPaging()).page(1).join();
        assertEquals(Lists.newArrayList(6), page3.getPage());
        assertNotNull(page3.getPaging());
        final PagedList<Integer> page4 = sort(page3.getPaging()).page(6).join();
        assertEquals(Lists.newArrayList(7, 8, 9, 10, 11, 12), page4.getPage());
        assertNotNull(page4.getPaging());
        final PagedList<Integer> page5 = sort(page4.getPaging()).page(10).join();
        assertEquals(Lists.newArrayList(13, 14, 15), page5.getPage());
        assertNull(page5.getPaging());
    }
}
