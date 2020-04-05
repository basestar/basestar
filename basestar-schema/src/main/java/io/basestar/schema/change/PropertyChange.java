package io.basestar.schema.change;

import io.basestar.schema.use.Use;
import lombok.Data;

public interface PropertyChange {

    @Data
    class ChangeType implements PropertyChange {

        private final Use<?> type;
    }

    @Data
    class ChangeRequired implements PropertyChange {

        private final boolean required;
    }
}
