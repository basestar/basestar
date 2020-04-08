package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestIndex {

    @Test
    public void testReadRecords() {

        final Index index = Index.builder()
                .setConsistency(Consistency.ASYNC)
                .setOver(ImmutableMap.of(
                        "a", Path.of("as"),
                        "b", Path.of("bs"),
                        "c", Path.of("cs")
                ))
                .setPartition(ImmutableList.of(
                    Path.of("a", "id"),
                    Path.of("b", "id"),
                    Path.of("c", "id"),
                    Path.of("d")
            )).setSort(ImmutableList.of(
                    Sort.asc(Path.of("e"))
            )).build("test");

        final Map<String, Object> data = new HashMap<>();
        data.put("as", ImmutableList.of(
                ImmutableMap.of("id", 1),
                ImmutableMap.of("id", 2),
                ImmutableMap.of("id", 3)
        ));
        data.put("bs", ImmutableList.of(
                ImmutableMap.of("id", 4),
                ImmutableMap.of("id", 5),
                ImmutableMap.of("id", 6)
        ));
        data.put("cs", ImmutableList.of(
                ImmutableMap.of("id", 7),
                ImmutableMap.of("id", 8)
        ));
        data.put("d", 9);
        data.put("e", 10);

        final Map<Index.Key, Map<String, Object>> records = index.readValues(data);

        assertEquals(ImmutableSet.of(
                ImmutableList.of(1, 4, 7, 9), ImmutableList.of(2, 4, 7, 9), ImmutableList.of(3, 4, 7, 9),
                ImmutableList.of(1, 5, 7, 9), ImmutableList.of(2, 5, 7, 9), ImmutableList.of(3, 5, 7, 9),
                ImmutableList.of(1, 6, 7, 9), ImmutableList.of(2, 6, 7, 9), ImmutableList.of(3, 6, 7, 9),
                ImmutableList.of(1, 4, 8, 9), ImmutableList.of(2, 4, 8, 9), ImmutableList.of(3, 4, 8, 9),
                ImmutableList.of(1, 5, 8, 9), ImmutableList.of(2, 5, 8, 9), ImmutableList.of(3, 5, 8, 9),
                ImmutableList.of(1, 6, 8, 9), ImmutableList.of(2, 6, 8, 9), ImmutableList.of(3, 6, 8, 9)
        ), records.keySet().stream()
                .map(Index.Key::getPartition).collect(Collectors.toSet()));

        assertEquals(ImmutableSet.of(
                ImmutableList.of(10)
        ), records.keySet().stream()
                .map(Index.Key::getSort).collect(Collectors.toSet()));

        assertEquals(ImmutableSet.of(
                data
        ), ImmutableSet.copyOf(records.values()));
    }
}
