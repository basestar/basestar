package io.basestar.storage;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.ReferableSchema;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestBatchResponse {

    @Test
    void testGet() {

        final BatchResponse response = BatchResponse.fromRefs(ImmutableMap.of(
                BatchResponse.RefKey.from(Name.of("Location"), ReferableSchema.ref("x")), ReferableSchema.ref("a")
        ));
        final Map<String, Object> c = response.get(Name.of("Task"), "y");
        assertNull(c);
        final Map<String, Object> d = response.get(Name.of("Location"), "x");
        assertNotNull(d);
    }
}
