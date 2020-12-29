package io.basestar.schema;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.use.*;

import java.util.Map;

public class SchemaTestUtils {

    public static Map<String, Property.Descriptor> scalarProperties() {

        return ImmutableMap.<String, Property.Descriptor>builder()
                .put("boolean", Property.builder().setType(UseBoolean.DEFAULT))
                .put("integer", Property.builder().setType(UseInteger.DEFAULT))
                .put("number", Property.builder().setType(UseNumber.DEFAULT))
                .put("string", Property.builder().setType(UseString.DEFAULT))
                .put("binary", Property.builder().setType(UseBinary.DEFAULT))
                .put("secret", Property.builder().setType(UseSecret.DEFAULT))
                .build();
    }
}
