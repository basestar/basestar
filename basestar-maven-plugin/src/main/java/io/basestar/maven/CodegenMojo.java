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

import io.basestar.codegen.Codegen;
import io.basestar.codegen.CodegenSettings;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.URLs;
import lombok.Setter;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Setter
@Mojo(name = "codegen", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CodegenMojo extends AbstractMojo {

    @Parameter(required = true, defaultValue = "java")
    private String language;

    @Parameter(required = true)
    private String packageName;

    @Parameter(required = false)
    private List<String> searchPackageNames;

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

            final List<String> searchPackageNames = new ArrayList<>();
            searchPackageNames.add(packageName);
            if(this.searchPackageNames != null) {
                searchPackageNames.addAll(this.searchPackageNames);
            }

            final Schema.Resolver resolver = new ClassLoadingResolver(searchPackageNames);
            final Namespace ns = Namespace.load(resolver, schemaUrls.stream().map(URLs::toURLUnchecked).toArray(URL[]::new));

            final CodegenSettings settings = CodegenSettings.builder()
                    .packageName(packageName)
                    .build();

            final Codegen codegen = new Codegen(language, settings);

            final File base = new File(outputDirectory);

            codegen.generate(ns, base, new Codegen.Log() {
                @Override
                public void info(final String message) {

                    getLog().info(message);
                }

                @Override
                public void error(final String message, final Throwable error) {

                    getLog().error(message, error);
                }
            });

            if(addSources && project != null) {
                getLog().info("Adding source directory " + base.getAbsolutePath());
                project.addCompileSourceRoot(base.getAbsolutePath());
            }

        } catch (final Exception e) {
            getLog().error("Codegen execution failed", e);
            throw new MojoExecutionException("Codegen execution failed", e);
        }
    }

    private static class ClassLoadingResolver implements Schema.Resolver.Constructing {

        private final MappingContext mappingContext = new MappingContext();

        private final List<Name> packageNames;

        private final Map<Name, Schema> schemas = new ConcurrentHashMap<>();

        public ClassLoadingResolver(final List<String> packageNames) {

            this.packageNames = Name.parseList(packageNames);
        }

        @Nullable
        @Override
        public Schema getSchema(final Name qualifiedName) {

            if (schemas.containsKey(qualifiedName)) {
                return schemas.get(qualifiedName);
            } else {
                for (final Name packageName : packageNames) {
                    try {
                        final Class<?> cls = Class.forName(packageName.with(qualifiedName).toString());
                        return mappingContext.schema(this, cls);
                    } catch (final ClassNotFoundException e) {
                    }
                }
                return null;
            }
        }

        @Override
        public void constructing(final Name qualifiedName, final Schema schema) {

            schemas.put(qualifiedName, schema);
        }
    }
}
