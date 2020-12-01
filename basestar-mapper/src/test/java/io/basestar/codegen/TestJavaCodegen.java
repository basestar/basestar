package io.basestar.codegen;

/*-
 * #%L
 * basestar-mapper
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

import io.basestar.schema.Namespace;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class TestJavaCodegen {

    private static final File base = new File("target/codegen");

    Namespace namespace() throws IOException {

        return Namespace.load(TestJavaCodegen.class.getResource("schema.yml"));
    }

    @Test
    void testCodegen() throws IOException {

        final Namespace namespace = namespace();

        final CodegenSettings settings = CodegenSettings.builder().packageName("io.basestar.test").build();
        final Codegen codegen = new Codegen("java", settings);

        codegen.generate(namespace, base);
    }
}
