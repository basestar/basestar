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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseOptional;
import io.basestar.schema.use.UseString;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;
import io.basestar.util.Page;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TestObjectSchema {

    @Test
    @Deprecated
    void testRequiredExpand() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        assertEquals(Name.parseSet("ref"), schema.requiredExpand(Name.parseSet("ref.ref.id")));
        assertEquals(Name.parseSet(""), schema.requiredExpand(Name.parseSet("ref.id")));
        assertEquals(Name.parseSet("ref.ref"), schema.requiredExpand(Name.parseSet("ref.ref.string")));

        assertEquals(Name.parseSet(""), schema.requiredExpand(Name.parseSet("string")));
    }

    @Test
    void testLegacy() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("legacy.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Legacy");

        assertEquals(UseString.DEFAULT, schema.requireProperty("name", true).typeOf());
        assertEquals(new UseOptional<>(UseInteger.DEFAULT), schema.requireProperty("age", true).typeOf());
    }

    @Test
    void testUniqueNames() throws IOException {
        SchemaValidationException schemaValidationException = assertThrows(SchemaValidationException.class,
                () -> Namespace.load(TestObjectSchema.class.getResource("conflictingNamesObject.yml")));

        assertTrue(schemaValidationException.getMessage().contains("my-name"));
        assertTrue(schemaValidationException.getMessage().contains("MyName"));
        assertTrue(schemaValidationException.getMessage().contains("MySurname"));
        assertTrue(schemaValidationException.getMessage().contains("MY_SURNAME"));
        assertFalse(schemaValidationException.getMessage().contains("first"));//assert that only properties at the same level is validated against each other
        assertFalse(schemaValidationException.getMessage().contains("key"));//assert that reserved keywords doesn't trigger validation
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testExpandCollapse() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        final String id = UUID.randomUUID().toString();
        final Instance initial = schema.create(ImmutableMap.of(
                "ref", ImmutableMap.of(
                        ObjectSchema.ID, id
                )
        ));

        final Instance refValue = schema.create(ImmutableMap.of(
                ObjectSchema.ID, UUID.randomUUID().toString()
        ));

        final Instance instance = schema.expand(initial, new Expander() {
            @Override
            public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                return Instance.getId(ref).equals(id) ? refValue : null;
            }

            @Override
            public Instance expandVersionedRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                return expandRef(name, schema, ref, expand);
            }

            @Override
            public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {

                return null;
            }
        }, ImmutableSet.of(Name.of("ref")));

        final Instance expanded = schema.expand(instance, Expander.noop(), ImmutableSet.of(Name.of("ref")));
        final Map expandedRef = (Map) expanded.get("ref");
        assertNotNull(expandedRef.get(ObjectSchema.SCHEMA));
        assertNotNull(expandedRef.get(ObjectSchema.ID));

        final Instance collapsed = schema.expand(instance, Expander.noop(), ImmutableSet.of());
        final Map collapsedRef = (Map) collapsed.get("ref");
        assertNull(collapsedRef.get(ObjectSchema.SCHEMA));
        assertNotNull(collapsedRef.get(ObjectSchema.ID));
    }

    @Test
    void testRefQueries() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Post");

        final Set<Expression> queries = schema.refQueries(Name.of("Post"), ImmutableSet.of(Name.of("ref")));
        assertEquals(ImmutableSet.of(new Eq(new NameConstant(Name.of("ref", "id")), new NameConstant(Name.of(Reserved.THIS, ObjectSchema.ID)))), queries);

        final Set<Expression> nonQueries = schema.refQueries(Name.of("Post"), ImmutableSet.of());
        assertEquals(ImmutableSet.of(), nonQueries);
    }

    @Test
    void testDependencies() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Comment");

        final Map<Name, Schema> deps = schema.dependencies();

        assertEquals(2, deps.size());
    }

    @Test
    void testBucketing() throws IOException {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("schema.yml"));

        final ObjectSchema comment = namespace.requireObjectSchema("Comment");
        assertEquals(ImmutableList.of(
                new Bucketing(ImmutableList.of(Name.of("created")), 20)
        ), comment.getDeclaredBucketing());

        final ObjectSchema post = namespace.requireObjectSchema("Post");
        assertEquals(ImmutableList.of(
                new Bucketing(ImmutableList.of(Name.of("created"))),
                new Bucketing(ImmutableList.of(Name.of("id")), 50)
        ), post.getDeclaredBucketing());
    }
}