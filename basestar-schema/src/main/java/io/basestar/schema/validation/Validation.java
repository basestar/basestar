package io.basestar.schema.validation;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.schema.use.Use;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Validation {

    Map<String, Validation> TYPES = ImmutableMap.<String, Validation>builder()
            .put(AssertValidation.TYPE, new AssertValidation())
            .put(MaxValidation.TYPE, new MaxValidation())
            .put(MinValidation.TYPE, new MinValidation())
            .put(PatternValidation.TYPE, new PatternValidation())
            .put(SizeValidation.TYPE, new SizeValidation())
            .build();

    String type();

    Class<? extends Validator> validatorClass();

    Optional<Validator> fromJsr380(Use<?> type, Annotation annotation);

    static Set<String> types() {

        return TYPES.keySet();
    }

    static Validation forType(final String name) {

        final Validation validation = TYPES.get(name);
        if(validation == null) {
            throw new IllegalStateException("Validator " + name + " is not available");
        }
        return validation;
    }

    static Optional<Validator> createJsr380Validator(final Use<?> type, final Annotation annotation) {

        for (final Validation factory : TYPES.values()) {
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
