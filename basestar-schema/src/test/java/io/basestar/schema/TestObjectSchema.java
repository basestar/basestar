package io.basestar.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.util.PagedList;
import io.basestar.util.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TestObjectSchema {

    @Test
    @Deprecated
    public void testRequiredExpand() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        assertEquals(Path.parseSet("ref"), schema.requiredExpand(Path.parseSet("ref.ref.id")));
        assertEquals(Path.parseSet(""), schema.requiredExpand(Path.parseSet("ref.id")));
        assertEquals(Path.parseSet("ref.ref"), schema.requiredExpand(Path.parseSet("ref.ref.string")));

        assertEquals(Path.parseSet(""), schema.requiredExpand(Path.parseSet("string")));
    }

    @Test
    public void testExpandCollapse() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        final String id = UUID.randomUUID().toString();
        final Instance initial = schema.create(ImmutableMap.of(
                "ref", ImmutableMap.of(
                        Reserved.ID, id
                )
        ));

        final Instance refValue = schema.create(ImmutableMap.of(
                Reserved.ID, UUID.randomUUID().toString()
        ));

        final Instance instance = schema.expand(initial, new Expander() {
            @Override
            public Instance ref(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                return Instance.getId(ref).equals(id) ? refValue : null;
            }

            @Override
            public PagedList<Instance> link(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

                return null;
            }
        },  ImmutableSet.of(Path.of("ref")));

        final Instance expanded = schema.expand(instance, Expander.noop(), ImmutableSet.of(Path.of("ref")));
        final Map expandedRef = (Map)expanded.get("ref");
        assertNotNull(expandedRef.get(Reserved.SCHEMA));
        assertNotNull(expandedRef.get(Reserved.ID));

        final Instance collapsed = schema.expand(instance, Expander.noop(), ImmutableSet.of());
        final Map collapsedRef = (Map)collapsed.get("ref");
        assertNull(collapsedRef.get(Reserved.SCHEMA));
        assertNotNull(collapsedRef.get(Reserved.ID));

    }
}