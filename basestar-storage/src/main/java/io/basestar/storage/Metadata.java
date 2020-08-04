package io.basestar.storage;

import io.basestar.schema.Instance;
import io.basestar.schema.Reserved;

import java.util.Map;

public interface Metadata {

    static <T extends Metadata> T readFrom(final Map<String, Object> object, final Class<T> of) {

        return object == null ? null : Instance.get(object, Reserved.META, of);
    }

    default <T> Map<String, Object> applyTo(final Map<String, Object> object) {

        return object;
//        return object == null ? null : Instance.with(object, Reserved.META, this);
    }

    default <T> Instance applyTo(final Instance object) {

        return object;
//        return object == null ? null : object.with(Reserved.META, this);
    }
}
