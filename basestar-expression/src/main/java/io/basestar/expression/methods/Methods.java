package io.basestar.expression.methods;

/*-
 * #%L
 * basestar-expression
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Methods {

    public static final List<Object> DEFAULT_FILTERS = ImmutableList.of(
            new StringMethods(), new ListMethods(), new SetMethods(), new MapMethods()
    );

    private final List<Object> filters;

    private final boolean whitelist;

    public Object call(final Object target, final String method, final Object ... args) {

        try {
            final Class<?> targetType = target.getClass();
            final Class<?>[] argTypes = Arrays.stream(args).map(Object::getClass).toArray(Class<?>[]::new);

            for(final Object filter : filters) {
                final Class<?>[] mergedTypes = ObjectArrays.concat(targetType, argTypes);
                final Method resolved = findMethod(filter.getClass(), method, mergedTypes);
                if(resolved != null) {
                    final Object[] mergedArgs = ObjectArrays.concat(target, args);
                    return resolved.invoke(filter, mergedArgs);
                }
            }

            if(whitelist) {
                throw new IllegalStateException("Cannot call " + method + " (restricted)");
            } else {
                final Method resolved = findMethod(targetType, method, argTypes);
                if(resolved != null) {
                    return resolved.invoke(target, args);
                } else {
                    throw new IllegalStateException("method not found");
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException("Cannot call method " + method + " on object " + target, e);
        }
    }

    private static Method findMethod(final Class<?> type, final String name, final Class<?>... args) {

        for(final Method method : type.getMethods()) {
            if(!method.getDeclaringClass().equals(Object.class) && method.getName().equals(name)) {
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

        private final List<Object> filters = new ArrayList<>();

        private boolean whitelist = true;

        public Builder whitelist(boolean whitelist) {

            this.whitelist = whitelist;
            return this;
        }

        public Builder filter(final Object filter) {

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
