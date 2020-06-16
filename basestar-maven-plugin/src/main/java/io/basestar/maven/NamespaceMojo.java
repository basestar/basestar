package io.basestar.maven;

import com.google.common.base.Charsets;
import io.basestar.mapper.MappingContext;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Setter
@Mojo(name = "namespace", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class NamespaceMojo extends AbstractMojo {

    @Parameter(required = true)
    private List<String> classes;

    @Parameter(required = true, defaultValue = "${project.build.directory}/generated-sources/basestar")
    private String outputDirectory;

    @Parameter
    private boolean addResources;

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void execute() throws MojoExecutionException {

        try {

            final Set<Class<?>> classes = new HashSet<>();
            for(final String name : this.classes) {
                classes.add(Class.forName(name));
            }

            final File output = new File(outputDirectory);
            output.mkdirs();

            final MappingContext context = new MappingContext();

            final Namespace.Builder all = context.namespace(classes);
            for(final Map.Entry<String, Schema.Builder<?>> entry : all.getSchemas().entrySet()) {
                final String name = entry.getKey();
                final Schema.Builder<?> schema = entry.getValue();
                final File file = new File(output, name + ".yml");
                final Namespace.Builder one = Namespace.builder()
                        .setSchema(name, schema);
                try(final FileOutputStream fos = new FileOutputStream(file);
                    final OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8)) {
                    getLog().info("Writing schema " + name + " to " + file.getAbsolutePath());
                    one.yaml(writer);
                }
            }

            if(addResources && project != null) {
                getLog().info("Adding resource directory " + output.getAbsolutePath());
                final Resource resource = new Resource();
                resource.setDirectory(outputDirectory);
                project.addResource(resource);
            }

        } catch (final Exception e) {
            getLog().error("Namespace execution failed", e);
            throw new MojoExecutionException("Namespace execution failed", e);
        }
    }
}
