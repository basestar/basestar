package io.basestar.jackson;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.jackson.serde.PathDeserializer;
import io.basestar.jackson.serde.SortDeserializer;
import io.basestar.util.Path;
import io.basestar.util.Sort;

public class BasestarModule extends SimpleModule {

    public BasestarModule() {

        super("Basestar", new Version(1, 0, 0, null, null, null));

        final ToStringSerializer toString = new ToStringSerializer();

        addSerializer(Path.class, toString);
        addDeserializer(Path.class, new PathDeserializer());

        addSerializer(Sort.class, toString);
        addDeserializer(Sort.class, new SortDeserializer());

        addSerializer(Expression.class, toString);
        addDeserializer(Expression.class, new ExpressionDeseriaizer());
    }
}
