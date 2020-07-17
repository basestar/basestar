package io.basestar.mapper.annotation;

import io.basestar.mapper.internal.LinkMapper;
import io.basestar.mapper.internal.annotation.MemberModifier;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberModifier(Sort.Modifier.class)
public @interface Sort {

    String[] value();

    @RequiredArgsConstructor
    class Modifier implements MemberModifier.Modifier<LinkMapper> {

        private final Sort annotation;

        @Override
        public LinkMapper modify(final LinkMapper mapper) {

            final List<io.basestar.util.Sort> sort = Arrays.stream(annotation.value()).map(io.basestar.util.Sort::parse).collect(Collectors.toList());
            return mapper.withSort(sort);
        }

        public static Sort from(final List<io.basestar.util.Sort> sort) {

            return new Sort() {

                @Override
                public Class<? extends Annotation> annotationType() {

                    return Sort.class;
                }

                @Override
                public String[] value() {

                    return sort.stream().map(io.basestar.util.Sort::toString).toArray(String[]::new);
                }
            };
        }
    }
}
