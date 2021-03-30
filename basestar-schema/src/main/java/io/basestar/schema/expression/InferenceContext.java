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

public interface InferenceContext {

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    Use<?> typeOf(Name name);

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Use<?> typeOf(final Expression expression) {

        return new InferenceVisitor(this).visit(expression);
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

        return name -> UseAny.DEFAULT;
    }

    default InferenceContext with(final Map<String, Use<?>> context) {

        return name -> {

            final String first = name.first();
            final Use<?> use = context.get(first);
            if(use != null) {
                return use.typeOf(name.withoutFirst());
            } else {
                return InferenceContext.this.typeOf(name);
            }
        };
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default TypedExpression<?> typed(final Expression expr) {

        return TypedExpression.from(expr, typeOf(expr));
    }

    class FromLayout implements InferenceContext {

        private final Layout layout;

        public FromLayout(final Layout layout) {

            this.layout = layout;
        }

        @Override
        public Use<?> typeOf(final Name name) {

            return layout.typeOf(name);
        }
    }

    @RequiredArgsConstructor
    class Overlay implements InferenceContext {

        private final InferenceContext parent;

        private final Map<String, InferenceContext> overrides;

        @Override
        public Use<?> typeOf(final Name name) {

            final String first = name.first();
            final InferenceContext override = overrides.get(first);
            if(override != null) {
                return override.typeOf(name.withoutFirst());
            } else {
                return parent.typeOf(name);
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
        public Use<?> typeOf(final Name name) {

            return contexts.stream().map(v -> (Use)v.typeOf(name))
                    .reduce(Use::commonBase).orElse(UseAny.DEFAULT);
        }
    }
}
