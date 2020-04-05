package io.basestar.exception;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Data
@Accessors(chain = true)
public class ExceptionMetadata {

    private int status;

    private String code;

    private String message;

    private final Map<String, Object> data = new HashMap<>();

    public ExceptionMetadata putData(final String key, final Object value) {

        data.put(key, value);
        return this;
    }

    public Map<String, Object> getData() {

        return data;
    }
}
