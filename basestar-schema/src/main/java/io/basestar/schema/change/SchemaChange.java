package io.basestar.schema.change;

import io.basestar.schema.Property;
import lombok.Data;

public interface SchemaChange {

    @Data
    class Rename implements SchemaChange {

        private final String name;
    }

    @Data
    class AddProperty implements SchemaChange {

        private final String name;

        private final Property attribute;
    }
}
