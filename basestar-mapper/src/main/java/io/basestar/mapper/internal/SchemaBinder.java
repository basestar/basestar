package io.basestar.mapper.internal;

import io.basestar.mapper.type.WithType;
import io.basestar.schema.Schema;

public interface SchemaBinder {

    String name(WithType<?> type);

    Schema.Builder schemaBuilder(WithType<?> type);
}
