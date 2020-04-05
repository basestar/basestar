package io.basestar.mapper.type;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface HasAnnotations {

    List<WithAnnotation<?>> annotations();

    @SuppressWarnings("unchecked")
    default <A extends Annotation> WithAnnotation<A> annotation(final Class<A> of) {

        return (WithAnnotation<A>)annotations().stream().filter(v -> v.erasedType().equals(of))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    default <A extends Annotation> List<WithAnnotation<A>> annotations(final Class<A> of) {

        return annotations().stream()
                .filter(v -> v.erasedType().equals(of))
                .map(v -> (WithAnnotation<A>)v)
                .collect(Collectors.toList());
    }

    static Predicate<HasAnnotations> match(final Class<? extends Annotation> of) {

        return v -> v.annotation(of) != null;
    }
}
