package io.basestar.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.encoding.FlatEncoding;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestEncoding {

    private static final Map<String, Object> INPUT = ImmutableMap.of(
            "map", ImmutableMap.of(
                    "key1", "value1",
                    "key2", "value2"
            ),
            "collection", ImmutableList.of(
                    "value3", "value4"
            ),
            "collectionMap", ImmutableList.of(
                    ImmutableMap.of(
                            "key3", "value5"
                    )
            ),
            "string", "value5",
            "mapMap", ImmutableMap.of(
                    "key4", ImmutableMap.of(
                            "key5", "value6"
                    )
            )
    );

    @Test
    void testFlatEncoding() {

        final FlatEncoding encoding = new FlatEncoding();
        final Map<String, Object> encoded = encoding.encode(INPUT);
        assertEquals(ImmutableMap.<String, Object>builder()
                .put("map.key1", "value1")
                .put("map.key2", "value2")
                .put("collection[0]", "value3")
                .put("collection[1]", "value4")
                .put("collectionMap[0].key3", "value5")
                .put("mapMap.key4.key5", "value6")
                .put("string", "value5").build(), encoded);
        final Map<String, Object> decoded = encoding.decode(encoded);
        assertEquals(INPUT, decoded);
    }
}
