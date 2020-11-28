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
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestIndex {

    @Test
    void testReadRecords() {

        final Index index = Index.builder()
                .setConsistency(Consistency.ASYNC)
                .setOver(ImmutableMap.of(
                        "a", Name.of("as"),
                        "b", Name.of("bs"),
                        "c", Name.of("cs")
                ))
                .setPartition(ImmutableList.of(
                    Name.of("a", "id"),
                    Name.of("b", "id"),
                    Name.of("c", "id"),
                    Name.of("d")
            )).setSort(ImmutableList.of(
                    Sort.asc(Name.of("e"))
            )).build(Name.of("test"));

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
        ).stream().map(partition -> Index.Key.of(partition, ImmutableList.of(10))).collect(Collectors.toSet()),
                records.keySet());

        assertEquals(ImmutableSet.of(
                data
        ), ImmutableSet.copyOf(records.values()));
    }
}
