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

import com.google.common.base.Charsets;
import io.basestar.codegen.Codegen;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import lombok.Setter;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Setter
@Mojo(name = "codegen", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CodegenMojo extends AbstractMojo {

    @Parameter(required = true, defaultValue = "java")
    private String language;

    @Parameter(required = true)
    private String packageName;

    @Parameter(required = true)
    private List<String> schemaUrls;

    @Parameter(required = true, defaultValue = "${project.build.directory}/generated-sources/basestar")
    private String outputDirectory;

    @Parameter
    private boolean addSources;

    @Parameter(defaultValue="${project}")
    private MavenProject project;

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void execute() throws MojoExecutionException {

        System.setProperty("java.protocol.handler.pkgs", "io.basestar.protocol");
        try {
            final Namespace ns = Namespace.load(schemaUrls.stream().map(v -> {
                try {
                    return new URL(v);
                } catch (final MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }).toArray(URL[]::new));

            final CodegenSettings settings = CodegenSettings.builder()
                    .packageName(packageName)
                    .build();

            final Codegen codegen = new Codegen(language, settings);

            final File base = new File(outputDirectory);
            base.mkdirs();

            for(final Schema<?> schema : ns.getSchemas().values()) {
                final File output = packageOutputDirectory(base, schema);
                output.mkdirs();
                final File file = new File(output, schema.getName() + ".java");
                try(final FileOutputStream fos = new FileOutputStream(file);
                    final OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8)) {
                    getLog().info("Writing schema " + schema.getQualifiedName() + " to " + file.getAbsolutePath());
                    codegen.generate(schema, writer);
                }
            }

            if(addSources && project != null) {
                getLog().info("Adding source directory " + base.getAbsolutePath());
                project.addCompileSourceRoot(base.getAbsolutePath());
            }

        } catch (final Exception e) {
            getLog().error("Codegen execution failed", e);
            throw new MojoExecutionException("Codegen execution failed", e);
        }
    }

    private File packageOutputDirectory(final File base, final Schema<?> schema) {

        final Name schemaPackageName = Name.parse(packageName).with(schema.getQualifiedPackageName());
        return new File(base, schemaPackageName.toString().replaceAll("\\.", File.separator));
    }
}
