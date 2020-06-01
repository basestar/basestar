package io.basestar.codegen;

import io.basestar.schema.Namespace;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

public class TestJavaCodegen {

    public Namespace namespace() throws IOException {

        return Namespace.load(TestJavaCodegen.class.getResource("schema.yml"));
    }

    @Test
    public void testEnumSchema() throws IOException {

        final Namespace namespace = namespace();

        final CodegenSettings settings = CodegenSettings.builder().packageName("io.basestar.test").build();
        final Codegen codegen = new Codegen("java", settings);

        final StringWriter stringWriter = new StringWriter();
        codegen.generate(namespace.requireSchema("MyEnum"), stringWriter);
        codegen.generate(namespace.requireSchema("MyObject"), stringWriter);
        System.err.println(stringWriter);
    }
}
