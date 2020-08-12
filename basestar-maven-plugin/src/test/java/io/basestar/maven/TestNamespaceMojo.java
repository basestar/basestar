package io.basestar.maven;

/*-
 * #%L
 * basestar-maven-plugin
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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNamespaceMojo {

    private final File namespaceDirectory = new File("target/test/namespace");

    @Test
    public void testNamespaceMojo() throws Exception {

        FileUtils.deleteDirectory(namespaceDirectory);

        final NamespaceMojo mojo = new NamespaceMojo();
        mojo.setSearchPackages(ImmutableList.of("io.basestar.maven"));
        mojo.setOutputDirectory(namespaceDirectory.toString());
        mojo.setOutputFilename("namespace.yml");
        mojo.setAddResources(false);
        mojo.setSchemaUrls(ImmutableList.of(
                "classpath:/io/basestar/maven/schema.yml"
        ));

        mojo.execute();

        final File file = new File(namespaceDirectory, "namespace.yml");

        assertTrue(file.exists());
    }
}
