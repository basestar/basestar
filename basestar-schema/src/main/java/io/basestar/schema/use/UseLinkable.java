package io.basestar.schema.use;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.ViewSchema;

public interface UseLinkable extends UseInstance {

    static UseLinkable from(final LinkableSchema schema, final Object config) {

        if(schema instanceof ReferableSchema) {
            return UseRef.from((ReferableSchema)schema, config);
        } else if(schema instanceof ViewSchema) {
            return UseView.from((ViewSchema)schema, config);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    LinkableSchema getSchema();
}
