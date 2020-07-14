package io.basestar.maven;

import com.google.common.collect.ImmutableList;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCodegenMojo {

    private final File outputDirectory = new File("target/test/codegen");

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testCodegenMojo() throws MojoExecutionException {

        outputDirectory.delete();

        final CodegenMojo mojo = new CodegenMojo();
        mojo.setLanguage("java");
        mojo.setPackageName("io.basestar.maven.test");
        mojo.setSchemaUrls(ImmutableList.of(
                "classpath:/io/basestar/maven/schema.yml"
        ));
        mojo.setOutputDirectory(outputDirectory.toString());
        mojo.setAddSources(false);

        mojo.execute();

        assertTrue(new File(outputDirectory, "io/basestar/maven/test/a/Test.java").exists());
        assertTrue(new File(outputDirectory, "io/basestar/maven/test/b/Test.java").exists());
        assertTrue(new File(outputDirectory, "io/basestar/maven/test/Test.java").exists());
    }
}
