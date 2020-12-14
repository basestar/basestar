package io.basestar.schema.use;

import io.basestar.schema.LinkableSchema;

public interface UseLinkable extends UseInstance {

    @Override
    LinkableSchema getSchema();
}
