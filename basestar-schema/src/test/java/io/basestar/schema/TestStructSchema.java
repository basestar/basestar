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

import io.basestar.schema.exception.SchemaValidationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStructSchema {

    @Test
    void testUniqueNames() throws IOException {
        SchemaValidationException schemaValidationException = assertThrows(SchemaValidationException.class,
                () -> Namespace.load(TestStructSchema.class.getResource("uniqueNamesStruct.yml")));

        assertTrue(schemaValidationException.getMessage().contains("my-name"));
        assertTrue(schemaValidationException.getMessage().contains("MyName"));

    }
}