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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.PathConstant;
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
    @SuppressWarnings("rawtypes")
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
            public Instance expandRef(final ObjectSchema schema, final Instance ref, final Set<Path> expand) {

                return Instance.getId(ref).equals(id) ? refValue : null;
            }

            @Override
            public PagedList<Instance> expandLink(final Link link, final PagedList<Instance> value, final Set<Path> expand) {

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

    @Test
    public void testRefQueries() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        final Set<Expression> queries = schema.refQueries("Post");

        assertEquals(ImmutableSet.of(new Eq(new PathConstant(Path.of("ref", "id")), new PathConstant(Path.of(Reserved.THIS, Reserved.ID)))), queries);
    }
}
