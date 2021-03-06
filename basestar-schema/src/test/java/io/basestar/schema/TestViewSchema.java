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
import io.basestar.schema.use.UseString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestViewSchema {

    @Test
    void testShorthandProperties() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("view.yml"));

        final ViewSchema schema = namespace.requireViewSchema("View");
        final Property emailProp = schema.getProperties().get("email");

        assertTrue(emailProp.typeOf() instanceof UseString);
        assertEquals(Expression.parse("email"), emailProp.getExpression());
    }
}
