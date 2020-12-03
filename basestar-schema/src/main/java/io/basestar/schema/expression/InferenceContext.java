package io.basestar.schema.expression;

import io.basestar.expression.call.Callable;
import io.basestar.expression.methods.Methods;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public interface InferenceContext {

    Use<?> typeOfMember(Name name);

    Use<?> typeOfCall(Use<?> target, String member, List<Use<?>> args);

    default InferenceContext with(final Map<String, Use<?>> context) {

        return new InferenceContext() {
            @Override
            public Use<?> typeOfMember(final Name name) {

                final String first = name.first();
                final Use<?> use = context.get(first);
                if(use != null) {
                    return use.typeOf(name.withoutFirst());
                } else {
                    return InferenceContext.this.typeOfMember(name);
                }
            }

            @Override
            public Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {

                return InferenceContext.this.typeOfCall(target, member, args);
            }
        };
    }

    @RequiredArgsConstructor
    class FromSchema implements InferenceContext {

        private final InstanceSchema schema;

        private final Methods methods;

        public FromSchema(final InstanceSchema schema) {

            this.schema = schema;
            this.methods = Methods.builder().defaults().build();
        }

        @Override
        public Use<?> typeOfMember(final Name name) {

            return schema.typeOf(name);
        }

        @Override
        public Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {

            final Type[] argTypes = args.stream().map(Use::javaType).toArray(Type[]::new);
            final Callable callable = methods.callable(target.javaType(), member, argTypes);
            return Use.fromJavaType(callable.type());
        }
    }

    @RequiredArgsConstructor
    class Overlay implements InferenceContext {

        private final InferenceContext parent;

        private final Map<String, InferenceContext> overrides;

        @Override
        public Use<?> typeOfMember(final Name name) {

            final String first = name.first();
            final InferenceContext override = overrides.get(first);
            if(override != null) {
                return override.typeOfMember(name.withoutFirst());
            } else {
                return parent.typeOfMember(name);
            }
        }

        @Override
        public Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {

            return parent.typeOfCall(target, member, args);
        }
    }
}
