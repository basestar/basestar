package io.basestar.schema;

import io.basestar.util.Immutable;
import org.junit.jupiter.api.Test;

public class TestSchema {

    @Test
    void testRebuild() throws Exception {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final Namespace rebuilt = Namespace.builder()
                .setSchemas(Immutable.transformValues(namespace.getSchemas(), (k, v) -> v.descriptor()))
                .build();
    }
}
