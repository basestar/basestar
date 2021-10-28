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

import io.basestar.expression.Expression;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.from.FromExternal;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestViewSchema {

    @Test
    void testShorthandProperties() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("View");
        final Property prop = schema.getProperties().get("email");

        assertTrue(prop.typeOf() instanceof UseString);
        assertEquals(Expression.parse("email"), prop.getExpression());
    }

    @Test
    void testUnionView() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("WithUnion");
        final Property prop = schema.getProperties().get("name");

        assertTrue(prop.typeOf() instanceof UseString);
    }

    @Test
    void testConflictingNames() throws IOException {
        SchemaValidationException schemaValidationException = assertThrows(SchemaValidationException.class,
                () -> Namespace.load(TestViewSchema.class.getResource("conflictingNamesView.yml")));

        assertTrue(schemaValidationException.getMessage().contains("my-name"));
        assertTrue(schemaValidationException.getMessage().contains("MyName"));
        assertTrue(schemaValidationException.getMessage().contains("MySurname"));
        assertTrue(schemaValidationException.getMessage().contains("MY_SURNAME"));
        assertFalse(schemaValidationException.getMessage().contains("first"));
        assertFalse(schemaValidationException.getMessage().contains("key"));//assert that reserved keywords doesn't trigger validation
    }

    @Test
    void testSqlView() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("WithSqlGroup");
        final Property prop = schema.getProperties().get("count");

        assertTrue(prop.typeOf() instanceof UseInteger);
    }

    @Test
    void testSqlNestedView() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("WithSqlNested");
        final Property prop = schema.getProperties().get("count");

        assertTrue(prop.typeOf() instanceof UseInteger);
    }

    @Test
    void testExternalView() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("ExternalView");

        assertTrue(schema.getFrom() instanceof FromExternal);

        final Property prop = schema.getProperties().get("value");

        assertTrue(prop.typeOf() instanceof UseString);
    }
}
