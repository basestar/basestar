package io.basestar.mapper.type;

import io.leangen.geantyref.GenericTypeReflector;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.function.Predicate;

public interface HasType<V> {

    AnnotatedType annotatedType();

    default WithType<V> type() {

        return WithType.with(annotatedType());
    }

    default Type genericType() {

        return annotatedType().getType();
    }

    @SuppressWarnings("unchecked")
    default Class<V> erasedType() {

        return (Class<V>)GenericTypeReflector.erase(genericType());
    }

    static Predicate<HasType<?>> match(final Class<?> raw) {

        return v -> raw.equals(v.erasedType());
    }

    static <V> Predicate<HasType<V>> match(final Predicate<WithType<V>> predicate) {

        return v -> predicate.test(v.type());
    }
}
