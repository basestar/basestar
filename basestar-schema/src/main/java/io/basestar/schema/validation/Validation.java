package io.basestar.schema.validation;

import io.basestar.expression.Context;
import io.basestar.schema.use.Use;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.*;

public interface Validation {

    ServiceLoader<Validation> LOADER = ServiceLoader.load(Validation.class);

    String type();

    Class<? extends Validator> validatorClass();

    Optional<Validator> fromJsr380(Use<?> type, Annotation annotation);

    static Set<String> types() {

        final Set<String> types = new HashSet<>();
        LOADER.forEach(factory -> types.add(factory.type()));
        return types;
    }

    static Validation forType(final String name) {

        for (final Validation factory : LOADER) {
            if(name.equals(factory.type())) {
                return factory;
            }
        }
        throw new IllegalStateException("Validator " + name + " is not available");
    }

    static Optional<Validator> createJsr380Validator(final Use<?> type, final Annotation annotation) {

        for (final Validation factory : LOADER) {
            final Optional<Validator> validator = factory.fromJsr380(type, annotation);
            if(validator.isPresent()) {
                return validator;
            }
        }
        return Optional.empty();
    }

    interface Validator extends Serializable {

        interface Builder {

            Validator build();
        }

        String type();

        String defaultMessage();

        boolean validate(Use<?> type, Context context, Object value);

        Annotation toJsr380(Use<?> type, Map<String, Object> values);

        // Simplified object for json output
        Object shorthand();
    }
}
