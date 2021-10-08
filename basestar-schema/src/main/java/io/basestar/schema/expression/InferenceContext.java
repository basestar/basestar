package io.basestar.schema.expression;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseAny;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Warnings;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface InferenceContext {

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    Optional<Use<?>> optionalTypeOf(Name name);

    default Use<?> typeOf(final Name name) {

        return optionalTypeOf(name).orElse(UseAny.DEFAULT);
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Optional<Use<?>> optionalTypeOf(final Expression expression) {

        return new InferenceVisitor(this).visit(expression);
    }

    default Use<?> typeOf(final Expression expression) {

        return optionalTypeOf(expression).orElse(UseAny.DEFAULT);
    }

    default Use<?> requireTypeOf(final Name name) {

        return optionalTypeOf(name).orElseThrow(() -> new IllegalStateException("Type of " + name + " cannot be inferred"));
    }

    default Use<?> requireTypeOf(final Expression expression) {

        return optionalTypeOf(expression).orElseThrow(() -> new IllegalStateException("Type of " + expression + " cannot be inferred"));
    }

    default InferenceContext overlay(final String name, final InferenceContext override) {

        return overlay(ImmutableMap.of(name, override));
    }

    default InferenceContext overlay(final Map<String, InferenceContext> overrides) {

        return new InferenceContext.Overlay(this, overrides);
    }

    static InferenceContext from(final Map<String, Use<?>> context) {

        return from(Layout.simple(context));
    }

    static InferenceContext from(final Layout layout) {

        return new FromLayout(layout);
    }

    static InferenceContext empty() {

        return name -> Optional.empty();
    }

    default InferenceContext with(final Map<String, Use<?>> context) {

        return name -> {

            final String first = name.first();
            final Use<?> use = context.get(first);
            if(use != null) {
                return Optional.of(use.typeOf(name.withoutFirst()));
            } else {
                return InferenceContext.this.optionalTypeOf(name);
            }
        };
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default TypedExpression<?> typed(final Expression expr) {

        return TypedExpression.from(expr, typeOf(expr));
    }

    default Use<?> namedType(final String type) {

        return Use.fromName(type);
    }

    class FromLayout implements InferenceContext {

        private final Layout layout;

        public FromLayout(final Layout layout) {

            this.layout = layout;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Optional<Use<?>> optionalTypeOf(final Name name) {

            return (Optional<Use<?>>)(Optional<?>)layout.optionalTypeOf(name);
        }
    }

    @RequiredArgsConstructor
    class Overlay implements InferenceContext {

        private final InferenceContext parent;

        private final Map<String, InferenceContext> overrides;

        @Override
        public Optional<Use<?>> optionalTypeOf(final Name name) {

            final String first = name.first();
            final InferenceContext override = overrides.get(first);
            if(override != null) {
                return override.optionalTypeOf(name.withoutFirst());
            } else {
                return parent.optionalTypeOf(name);
            }
        }
    }

    class Union implements InferenceContext {

        private final List<InferenceContext> contexts;

        public Union(final List<InferenceContext> contexts) {

            this.contexts = Immutable.list(contexts);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Optional<Use<?>> optionalTypeOf(final Name name) {

            // FIXME: should use optionalTypeOf rather than typeOf since handling of Any will not work in join context
            return Optional.ofNullable((Use<?>)contexts.stream().map(v -> (Use)v.typeOf(name))
                    .reduce(Use::commonBase).orElse(null));
        }
    }

    class Join implements InferenceContext {

        private final InferenceContext left;

        private final InferenceContext right;

        public Join(final InferenceContext left, final InferenceContext right) {

            this.left = left;
            this.right = right;
        }

        @Override
        public Optional<Use<?>> optionalTypeOf(final Name name) {

            final Optional<Use<?>> l = left.optionalTypeOf(Name.of(name.first()));
            final Optional<Use<?>> r = right.optionalTypeOf(Name.of(name.first()));
            if(l.isPresent() && r.isPresent()) {
                throw new IllegalStateException("Ambiguous name " + name);
            } else if(l.isPresent()) {
                return left.optionalTypeOf(name);
            } else {
                return right.optionalTypeOf(name);
            }
        }
    }
}
