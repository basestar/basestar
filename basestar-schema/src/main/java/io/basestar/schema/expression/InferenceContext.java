package io.basestar.schema.expression;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Layout;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseAny;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.util.Map;

public interface InferenceContext {

    Use<?> typeOf(Name name);

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

//    class FromSchema implements InferenceContext {
//
//        private final InstanceSchema schema;
//
//        private final Methods methods;
//
//        public FromSchema(final InstanceSchema schema, final Methods methods) {
//
//            this.schema = schema;
//            this.methods = Methods.builder().defaults().build();
//        }
//
//        @Override
//        public Use<?> typeOf(final Name name) {
//
//            return schema.typeOf(name);
//        }
//
//        @Override
//        public Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {
//
//            final Type[] argTypes = args.stream().map(Use::javaType).toArray(Type[]::new);
//            final Callable callable = methods.callable(target.javaType(), member, argTypes);
//            return Use.fromJavaType(callable.type());
//        }
//    }

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
}
