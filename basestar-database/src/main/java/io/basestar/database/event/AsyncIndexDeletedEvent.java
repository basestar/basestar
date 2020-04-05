package io.basestar.database.event;

import io.basestar.schema.Index;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class AsyncIndexDeletedEvent implements ObjectEvent {

    private String schema;

    private String index;

    private String id;

    private long version;

    private Index.Key key;

    public static AsyncIndexDeletedEvent of(final String schema, final String index, final String id,
                                            final long version, final Index.Key key) {

        return new AsyncIndexDeletedEvent().setSchema(schema).setIndex(index).setId(id)
                .setVersion(version).setKey(key);
    }

    @Override
    public AsyncIndexDeletedEvent abbreviate() {

        return this;
    }
}
