package io.basestar.schema;

import lombok.Data;

@Data
public class Ref {

    private final String schema;

    private final String id;

    public static Ref of(final String schema, final String id) {

        return new Ref(schema, id);
    }
}
