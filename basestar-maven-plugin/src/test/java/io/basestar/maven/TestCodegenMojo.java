package io.basestar.maven;

import com.google.common.collect.ImmutableList;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.Test;

import java.io.File;

public class TestCodegenMojo {

    @Test
    public void testGenerate() throws MojoExecutionException {

        final CodegenMojo mojo = new CodegenMojo();
        mojo.setLanguage("java");
        mojo.setSchemaUrls(ImmutableList.of("classpath:schema.yml"));
        mojo.setPackageName("io.basestar.maven.example");
        mojo.setOutputDirectory(new File("target/test").getAbsolutePath());
        mojo.execute();
    }
}
