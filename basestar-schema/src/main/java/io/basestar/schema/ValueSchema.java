package io.basestar.schema;

import io.basestar.expression.Context;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Set;

public interface ValueSchema<T> extends Schema {

    interface Descriptor<S extends ValueSchema<V>, V> extends Schema.Descriptor<S> {

        interface Self<S extends ValueSchema<V>, V> extends Descriptor<S, V>, Schema.Descriptor.Self<S> {

        }
    }

    interface Builder<B extends Schema.Builder<B, S>, S extends ValueSchema<V>, V> extends Schema.Builder<B, S>, Descriptor<S, V>, Described.Builder<B>, Extendable.Builder<B> {

    }

    default T create(final Object value) {

        return create(value, Collections.emptySet(), false);
    }

    default T create(final Object value, final Set<Name> expand, final boolean suppress) {

        return create(ValueContext.standardOrSuppressing(suppress), value, expand);
    }

    T create(ValueContext context, Object value, Set<Name> expand);

    default Set<Constraint.Violation> validate(final Context context, final T after) {

        return validate(context, Name.empty(), after);
    }

    Set<Constraint.Violation> validate(Context context, Name name, T after);

    Type javaType(Name name);

    io.swagger.v3.oas.models.media.Schema<?> openApi();

    Use<T> typeOf();

    String toString(T value);
}
