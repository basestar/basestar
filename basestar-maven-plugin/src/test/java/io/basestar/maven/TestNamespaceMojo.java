//package io.basestar.maven;
//
//import com.google.common.collect.ImmutableList;
//import org.apache.maven.plugin.MojoExecutionException;
//import org.junit.jupiter.api.Test;
//
//import java.io.File;
//
//public class TestNamespaceMojo {
//
//    private final File outputDirectory = new File("target/test/namespace");
//
//    @Test
//    @SuppressWarnings("ResultOfMethodCallIgnored")
//    public void testNamespaceMojo() throws MojoExecutionException {
//
//        outputDirectory.delete();
//
//        final NamespaceMojo mojo = new NamespaceMojo();
//        mojo.setOutputDirectory(outputDirectory.toString());
//        mojo.setClasses(ImmutableList.of(
//                io.basestar.maven.test.a.Test.class.getName(),
//                io.basestar.maven.test.b.Test.class.getName(),
//                io.basestar.maven.test.Test.class.getName()
//        ));
//
//        mojo.execute();
//    }
//}
