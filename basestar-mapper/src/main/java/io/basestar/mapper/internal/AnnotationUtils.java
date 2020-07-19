package io.basestar.mapper.internal;

import java.util.Collection;

public class AnnotationUtils {

    public static String[] stringArray(final Collection<?> collection) {

        return collection.stream().map(Object::toString).toArray(String[]::new);
    }
}
