package io.basestar.schema.layout;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.StructSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestExpandedLayout {

    private Layout layout() throws IOException {

        final Namespace namespace = Namespace.load(TestFlatStructLayout.class.getResource("expanded.yml"));
        final InstanceSchema schema = namespace.requireInstanceSchema("Base");
        return new ExpandedLayout(schema);
    }

    private static void printLayout(final Name name, final Map<String, Use<?>> layout) throws IOException {

        final StructSchema struct = StructSchema.from(name, layout);
        Namespace.builder().setSchema(name, struct.descriptor()).yaml(System.err);
    }

    @Test
    public void testSchema() throws IOException {

        final Layout layout = layout();

        final Set<Name> expand = Name.parseSet("a.b1.c", "a.b2.c");
        final Map<String, Use<?>> result = layout.layoutSchema(expand);

        printLayout(Name.of("Base"), result);

        assertEquals(1, result.size());
    }
}
