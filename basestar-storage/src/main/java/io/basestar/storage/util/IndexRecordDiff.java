//package io.basestar.storage.util;
//
///*-
// * #%L
// * basestar-storage
// * %%
// * Copyright (C) 2019 - 2020 Basestar.IO
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import com.google.common.collect.Sets;
//import io.basestar.schema.Index;
//import lombok.Data;
//
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//@Data
//public class IndexRecordDiff {
//
//    private final Map<Index.Key, Map<String, Object>> create;
//
//    private final Map<Index.Key, Map<String, Object>> update;
//
//    private final Set<Index.Key> delete;
//
//    public static IndexRecordDiff from(final Map<Index.Key, Map<String, Object>> before,
//                                       final Map<Index.Key, Map<String, Object>> after) {
//
//        return new IndexRecordDiff(
//                Sets.difference(after.keySet(), before.keySet()).stream()
//                        .collect(Collectors.toMap(k -> k, after::get)),
//                Sets.intersection(after.keySet(), before.keySet()).stream()
//                        .collect(Collectors.toMap(k -> k, after::get)),
//                Sets.difference(before.keySet(), after.keySet())
//        );
//    }
//}
