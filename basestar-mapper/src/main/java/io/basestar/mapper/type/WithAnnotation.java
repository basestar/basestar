package io.basestar.mapper.type;

import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.AnnotatedType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Accessors(fluent = true)
public class WithAnnotation<A extends Annotation> implements HasType<A> {

    private final AnnotatedType annotatedType;

    private final A annotation;

    protected WithAnnotation(final A annotation) {

        final Class<?> rawType = annotation.annotationType();
        this.annotatedType = GenericTypeReflector.annotate(rawType);
        this.annotation = annotation;
    }

    public static List<WithAnnotation<?>> from(final AnnotatedElement element) {

        return Arrays.stream(element.getAnnotations())
                .map(WithAnnotation::new)
                .collect(Collectors.toList());
    }
}