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
import io.basestar.mapper.MappingContext;
import io.basestar.mapper.annotation.*;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.URLs;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import lombok.Setter;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Setter
@Mojo(name = "namespace", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class NamespaceMojo extends AbstractMojo {

    private final Class<?>[] SCHEMA_CLASSES = new Class<?>[]{ObjectSchema.class, StructSchema.class, ViewSchema.class, EnumSchema.class, SqlViewSchema.class};

    @Parameter
    private List<String> classes;

    @Parameter
    private List<String> searchPackages;

    @Parameter
    private List<String> schemaUrls;

    @Parameter(required = true, defaultValue = "${project.build.directory}/generated-resources/basestar")
    private String outputDirectory;

    @Parameter
    private String outputFilename;

    @Parameter
    private boolean addResources;

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    @Override
    public void execute() throws MojoExecutionException {

        execute(null);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void execute(final ClassLoader classLoader) throws MojoExecutionException {

        System.setProperty("java.protocol.handler.pkgs", "io.basestar.protocol");
        try {

            final Set<Class<?>> classes = new HashSet<>();
            if(this.classes != null) {
                for (final String name : this.classes) {
                    classes.add(classLoader == null ? Class.forName(name) : Class.forName(name, true, classLoader));
                }
            }
            if(this.searchPackages != null) {
                try (final ScanResult scanResult = new ClassGraph().enableClassInfo().enableAnnotationInfo()
                                     .acceptPackages(searchPackages.toArray(new String[0])).scan()) {
                    for(final Class<?> annotation : SCHEMA_CLASSES) {
                        for (final ClassInfo objectSchema : scanResult.getClassesWithAnnotation(annotation.getName())) {
                            classes.add(objectSchema.loadClass());
                        }
                    }
                }
            }

            final File base = new File(outputDirectory);
            base.mkdirs();

            final MappingContext context = new MappingContext();

            final Namespace.Builder all = context.namespace(classes);
            if(schemaUrls != null) {
                final Namespace.Builder include = Namespace.Builder.load(schemaUrls.stream().map(URLs::toURLUnchecked).toArray(URL[]::new));
                include.getSchemas().forEach(all::setSchema);
            }

            if(outputFilename == null || outputFilename.isEmpty()) {
                for(final Map.Entry<Name, Schema.Descriptor<?, ?>> entry : all.getSchemas().entrySet()) {
                    final Name name = entry.getKey();
                    final Schema.Descriptor<?, ?> schema = entry.getValue();
                    final File output = packageOutputDirectory(name);
                    output.mkdirs();
                    final File file = new File(output, name.last() + ".yml");
                    final Namespace.Builder one = Namespace.builder()
                            .setSchema(name, schema);
                    try(final FileOutputStream fos = new FileOutputStream(file);
                        final OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8)) {
                        getLog().info("Writing schema " + name + " to " + file.getAbsolutePath());
                        one.yaml(writer);
                    }
                }
            } else {
                final File output = new File(outputDirectory);
                output.mkdirs();
                final File file = new File(output, outputFilename);
                try(final FileOutputStream fos = new FileOutputStream(file);
                    final OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8)) {
                    getLog().info("Writing namespace to " + file.getAbsolutePath());
                    all.yaml(writer);
                }
            }

            if(addResources && project != null) {
                getLog().info("Adding resource directory " + base.getAbsolutePath());
                final Resource resource = new Resource();
                resource.setDirectory(outputDirectory);
                project.addResource(resource);
            }

        } catch (final Exception e) {
            getLog().error("Namespace execution failed", e);
            throw new MojoExecutionException("Namespace execution failed", e);
        }
    }

    private File packageOutputDirectory(final Name name) {

        final Name schemaPackageName = name.withoutLast();
        return new File(outputDirectory, schemaPackageName.toString().replaceAll("\\.", File.separator));
    }
}
