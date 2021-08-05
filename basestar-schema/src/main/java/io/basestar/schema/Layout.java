package io.basestar.schema;

import com.google.common.collect.ImmutableSet;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseAny;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Layout extends Serializable {

    Map<String, Use<?>> getSchema();

    Set<Name> getExpand();

    @SuppressWarnings("unchecked")
    default <T> Use<T> typeOf(final Name name) {

        return (Use<T>)optionalTypeOf(name).orElse(UseAny.DEFAULT);
    }

    <T> Optional<Use<T>> optionalTypeOf(Name name);

    static Layout simple(final Map<String, Use<?>> schema) {

        return simple(schema, ImmutableSet.of());
    }

    static Layout simple(final Map<String, Use<?>> schema, final Set<Name> expand) {

        return new Simple(schema, expand);
    }

    @Data
    class Simple implements Layout {

        private final Map<String, Use<?>> schema;

        private final Set<Name> expand;

        public Simple(final Map<String, Use<?>> schema, final Set<Name> expand) {

            this.schema = Immutable.map(schema);
            this.expand = Immutable.set(expand);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<Use<T>> optionalTypeOf(final Name name) {

            final String first = name.first();
            final Use<?> use = schema.get(first);
            if(use != null) {
                return Optional.of((Use<T>)use.typeOf(name.withoutFirst()));
            } else {
                return Optional.empty();
            }
        }
    }
}
