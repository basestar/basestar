package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Data;

public interface Version {

    @JsonValue
    String toString();

    @Data
    class Simple {

        private final String value;

        @Override
        public String toString() {

            return value;
        }
    }
}
