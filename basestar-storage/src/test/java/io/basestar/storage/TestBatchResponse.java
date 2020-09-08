package io.basestar.storage;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestBatchResponse {

    @Test
    public void testGet() {

        final BatchResponse response = new BatchResponse.Basic(ImmutableMap.of(
                BatchResponse.Key.from(Name.of("Location"), ObjectSchema.ref("x")), ObjectSchema.ref("a")
        ));
        final Map<String, Object> c = response.getObject(Name.of("Task"), "y");
        assertNull(c);
        final Map<String, Object> d = response.getObject(Name.of("Location"), "x");
        assertNotNull(d);
    }
}
