package io.basestar.maven;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Namespace;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.Test;

import javax.tools.*;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCodegenMojo {

    private final File codegenDirectory = new File("target/test/codegen");
    private final File namespaceDirectory = new File("target/test/namespace");

    @Test
    public void testCodegenMojo() throws MojoExecutionException, Exception {

        FileUtils.deleteDirectory(codegenDirectory);
        FileUtils.deleteDirectory(namespaceDirectory);

        final CodegenMojo codegen = new CodegenMojo();
        codegen.setLanguage("java");
        codegen.setPackageName("io.basestar.maven.test");
        codegen.setSchemaUrls(ImmutableList.of(
                "classpath:/io/basestar/maven/schema.yml"
        ));
        codegen.setOutputDirectory(codegenDirectory.toString());
        codegen.setAddSources(false);

        codegen.execute();

        final List<String> classes = ImmutableList.of(
                "io.basestar.maven.test.a.Test",
                "io.basestar.maven.test.b.Test",
                "io.basestar.maven.test.Test"
        );

        final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

        final List<File> files = classes.stream().map(this::verifyFile).collect(Collectors.toList());

        final Iterable<? extends JavaFileObject> compilationUnit = fileManager.getJavaFileObjectsFromFiles(files);
        final JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, Collections.emptyList(), null, compilationUnit);
        task.call();

        final URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { codegenDirectory.toURI().toURL() });

        final NamespaceMojo namespace = (NamespaceMojo)Class.forName("io.basestar.maven.NamespaceMojo", true, classLoader).newInstance();
        namespace.setOutputDirectory(namespaceDirectory.toString());
        namespace.setClasses(classes);

        namespace.execute(classLoader);

        assertEquals(Namespace.Builder.load(
                new URI("classpath:/io/basestar/maven/schema.yml").toURL()
        ), Namespace.Builder.load(
                new File(namespaceDirectory, "a/Test.yml").toURI().toURL(),
                new File(namespaceDirectory, "b/Test.yml").toURI().toURL(),
                new File(namespaceDirectory, "Test.yml").toURI().toURL()
        ));
    }

    private File verifyFile(final String name) {

        final File file = new File(codegenDirectory, name.replaceAll("\\.", "/") + ".java");
        assertTrue(file.exists(), "File " + file + " does not exist");
        return file;
    }
}
