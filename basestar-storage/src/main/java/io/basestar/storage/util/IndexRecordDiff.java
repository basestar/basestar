package io.basestar.storage.util;

import com.google.common.collect.Sets;
import io.basestar.schema.Index;
import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class IndexRecordDiff {

    private final Map<Index.Key, Map<String, Object>> create;

    private final Map<Index.Key, Map<String, Object>> update;

    private final Set<Index.Key> delete;

    public static IndexRecordDiff from(final Map<Index.Key, Map<String, Object>> before,
                                       final Map<Index.Key, Map<String, Object>> after) {

        return new IndexRecordDiff(
                Sets.difference(after.keySet(), before.keySet()).stream()
                        .collect(Collectors.toMap(k -> k, after::get)),
                Sets.union(after.keySet(), before.keySet()).stream()
                        .collect(Collectors.toMap(k -> k, after::get)),
                Sets.difference(before.keySet(), after.keySet())
        );
    }
}
