package io.basestar.mapper.type;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface HasParameters {

    List<WithParameter<?>> parameters();

    default List<Class<?>> erasedParameterTypes() {

        return parameters().stream().map(HasType::erasedType)
                .collect(Collectors.toList());
    }

    static Predicate<HasParameters> match(final Class<?> ... raw) {

        return match(Arrays.asList(raw));
    }

    static Predicate<HasParameters> match(final List<Class<?>> raw) {

        return v -> v.erasedParameterTypes().equals(raw);
    }

    static Predicate<HasParameters> match(final int count) {

        return v -> v.parameters().size() == count;
    }

}
