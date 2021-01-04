package io.basestar.schema;

import com.google.common.collect.ImmutableSet;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSchema {

    @Test
    void testRebuild() throws Exception {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final Namespace rebuilt = Namespace.builder()
                .setSchemas(Immutable.transformValues(namespace.getSchemas(), (k, v) -> v.descriptor()))
                .build();
    }

    @Test
    void testMaterializationDependencies() throws Exception {

        final Namespace namespace = Namespace.load(TestSchema.class.getResource("dependencies.yml"));

        final ViewSchema view = namespace.requireViewSchema("DerivedView");

        final Set<Name> deps = view.materializationDependencies(view.getExpand()).keySet();

        assertEquals(ImmutableSet.of(Name.of("BaseView"), Name.of("Object1"), Name.of("Object2"),
                Name.of("Linked"), Name.of("Ref1"), Name.of("Ref2"), Name.of("Ref3")), deps);
    }
}
