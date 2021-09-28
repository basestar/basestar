package io.basestar.schema.use;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.schema.Constraint;
import io.basestar.util.Name;

import java.util.Collections;
import java.util.Set;

public interface UseValidated<T> {

    default Set<Constraint.Violation> validate(final Context context, final Name name, final T value) {
        if (value == null) {
            return ImmutableSet.of(new Constraint.Violation(name, Constraint.REQUIRED, null, ImmutableSet.of()));
        }
        return validateType(context, name, value);
    }

    default Set<Constraint.Violation> validateType(final Context context, final Name name, final T value) {
        return Collections.emptySet();
    }
}
