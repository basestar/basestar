package io.basestar.expression.methods;

/*-
 * #%L
 * basestar-expression
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import io.basestar.expression.call.Callable;
import io.basestar.util.Name;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Methods implements Serializable {

    public static final List<Serializable> DEFAULT_FILTERS = ImmutableList.of(
            new ObjectMethods(), new StringMethods(), new ListMethods(), new SetMethods(),
            new NumberMethods(), new MapMethods(), new DateMethods(), new DateTimeMethods()
    );

    private final List<Serializable> filters;

    private final boolean whitelist;

    public Callable callable(final Type target, final String method, final Type[] args) {

        final Class<?> targetType = GenericTypeReflector.erase(target);
        final Class<?>[] argTypes = Arrays.stream(args).map(GenericTypeReflector::erase).toArray(Class<?>[]::new);

        for(final Object filter : filters) {
            final Class<?>[] mergedTypes = ObjectArrays.concat(targetType, argTypes);
            final Method resolved = findMethod(filter.getClass(), method, mergedTypes);
            if(resolved != null) {
                return callable(resolved, filter);
            }
        }

        if(whitelist) {
            throw new IllegalStateException("Cannot call non-whitelisted method " + methodString(method, args));
        } else {
            final Method resolved = findMethod(targetType, method, argTypes);
            if(resolved != null) {
                return callable(resolved, null);
            } else {
                throw new IllegalStateException("Method " + methodString(method, args) + " not found");
            }
        }
    }

    private String methodString(final String method, final Type[] args) {

        return method + "(" + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }

    public Callable callable(final Name name, final Type[] args) {

        final String method = name.toString();

        final Class<?>[] argTypes = Arrays.stream(args).map(GenericTypeReflector::erase).toArray(Class<?>[]::new);

        for(final Object filter : filters) {
            final Method resolved = findMethod(filter.getClass(), method, argTypes);
            if(resolved != null) {
                return callable(resolved, filter);
            }
        }

        throw new IllegalStateException("Cannot call non-whitelisted method " + methodString(method, args));
    }

    private static Callable callable(final Method method, final Object target) {

        // Callable must be serializable, so have to re-acquire it
        final String name = method.getName();
        final Class<?> declaring = method.getDeclaringClass();
        final Class<?>[] argTypes = method.getParameterTypes();

        return new Callable() {

            private Method method() {

                try {
                    return declaring.getMethod(name, argTypes);
                } catch (final NoSuchMethodException e) {
                    throw new IllegalStateException("Cannot acquire method " + name, e);
                }
            }

            @Override
            public Object call(final Object... args) {

                final Method method = method();
                final Object actualTarget;
                final Object[] actualArgs;
                if(target == null) {
                    actualTarget = args[0];
                    actualArgs = Arrays.stream(args).skip(1).toArray();
                } else {
                    actualTarget = target;
                    actualArgs = args;
                }
                if(actualTarget != null) {
                    try {
                        return method.invoke(actualTarget, actualArgs);
                    } catch (final Exception e) {
                        throw new IllegalStateException("Cannot call method " + method + " on object " + actualTarget, e);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public Type type() {

                return GenericTypeReflector.getReturnType(method(), declaring);
            }

            @Override
            public Type[] args() {

                return GenericTypeReflector.getParameterTypes(method(), declaring);
            }
        };
    }

    private static Method findMethod(final Class<?> type, final String name, final Class<?>... args) {

        for(final Method method : type.getMethods()) {
            if(!method.getDeclaringClass().equals(Object.class) && method.getName().equalsIgnoreCase(name)) {
                final Class<?>[] methodArgs = method.getParameterTypes();
                if(args.length == methodArgs.length) {
                    boolean matched = true;
                    for (int i = 0; i != args.length; ++i) {
                        final Class<?> methodArg = methodArgs[i];
                        final Class<?> arg = args[i];
                        matched = matched && methodArg.isAssignableFrom(arg);
                    }
                    if(matched) {
                        return method;
                    }
                }
            }
        }
        return null;
    }

    public static Methods.Builder builder() {

        return new Builder();
    }

    public static class Builder {

        private final List<Serializable> filters = new ArrayList<>();

        private boolean whitelist = true;

        public Builder whitelist(final boolean whitelist) {

            this.whitelist = whitelist;
            return this;
        }

        public Builder filter(final Serializable filter) {

            filters.add(filter);
            return this;
        }

        public Builder defaults() {

            filters.addAll(DEFAULT_FILTERS);
            return this;
        }

        public Methods build() {

            return new Methods(ImmutableList.copyOf(filters), whitelist);
        }
    }
}
