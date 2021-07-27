package io.basestar.expression;

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

import io.basestar.expression.call.Callable;
import io.basestar.expression.exception.MemberNotFoundException;
import io.basestar.expression.methods.Methods;
import io.basestar.expression.type.Coercion;
import io.basestar.util.Name;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public interface Context extends Serializable {

    static Context init() {

        return init(Collections.emptyMap());
    }

    static Context init(final Map<String, ?> scope) {

        return init(Methods.builder().defaults().build(), scope);
    }

    static Context init(final Methods methods) {

        return init(methods, Collections.emptyMap());
    }

    static Context init(final Methods methods, final Map<String, ?> scope) {

        final Map<String, Object> scopeCopy = new HashMap<>(scope);

        return new Context() {
            @Override
            public Object get(final String name) {

                if(scopeCopy.containsKey(name)) {
                    return scopeCopy.get(name);
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public boolean has(final String name) {

                return scopeCopy.containsKey(name);
            }

            @Override
            public Callable callable(final Type target, final String method, final Type... args) {

                return methods.callable(target, method, args);
            }

            @Override
            public Callable callable(final String method, final Type... args) {

                if(scopeCopy.containsKey(method)) {
                    return (Callable) scopeCopy.get(method);
                } else {
                    return methods.callable(Name.of(method), args);
                }
            }

            @Override
            public Object cast(final Object value, final String type) {

                // FIXME:
                switch (type.toLowerCase()) {
                    case "string":
                        return Coercion.toString(value);
                    case "date":
                        return Coercion.toDate(value);
                    case "datetime":
                        return Coercion.toDateTime(value);
                    case "int":
                    case "long":
                        return Coercion.toInteger(value);
                    case "float":
                    case "double":
                        return Coercion.toFloat(value);
                    case "decimal":
                        return Coercion.toDecimal(value);
                    case "boolean":
                        return Coercion.toBoolean(value);
                    default:
                        throw new UnsupportedOperationException("Cannot cast to " + type);
                }
            }
        };
    }

    static Context delegating(final Context delegate, final Map<String, ?> scope) {

        final Map<String, Object> scopeCopy = new HashMap<>(scope);

        return new Context() {
            @Override
            public Object get(final String name) {

                if(scopeCopy.containsKey(name)) {
                    return scopeCopy.get(name);
                } else {
                    return delegate.get(name);
                }
            }

            @Override
            public boolean has(final String name) {

                return scopeCopy.containsKey(name) || delegate.has(name);
            }

            @Override
            public Callable callable(final Type target, final String method, final Type... args) {

                return delegate.callable(target, method, args);
            }

            @Override
            public Callable callable(final String method, final Type... args) {

                if(scopeCopy.containsKey(method)) {
                    return (Callable) scopeCopy.get(method);
                } else {
                    return delegate.callable(method, args);
                }
            }

            @Override
            public Object cast(final Object value, final String type) {

                return delegate.cast(value, type);
            }
        };
    }

    Object get(String name);

    boolean has(String name);

    default Context with(final Map<String, ?> scope) {

        return Context.delegating(this, scope);
    }

    default Context with(final String name, final Object value) {

        return with(Collections.singletonMap(name, value));
    }

    default Object call(final Object target, final String method, final Object... args) {

        final Object[] mergedArgs = Stream.concat(Stream.of(target), Arrays.stream(args)).toArray();
        return callable(target == null ? Void.class : target.getClass(), method, Arrays.stream(args).map(Object::getClass).toArray(Type[]::new))
                .call(mergedArgs);
    }

    default Type callType(final Type target, final String method, final Type... args) {

        return callable(target, method, args).type();
    }

    Callable callable(Type target, String method, Type... args);

    Callable callable(String method, Type... args);

    default Object member(final Object target, final String member) {

        if(target == null) {
            return null;
        } else if(target instanceof Map<?, ?>) {
            return ((Map<?, ?>) target).get(member);
        } else {
            try {
                final String method = "get" + member.substring(0, 1).toUpperCase() + member.substring(1);
                return call(target, method);
            } catch (final MemberNotFoundException e) {
                throw new MemberNotFoundException(target.getClass(), member);
            }
        }
    }

    Object cast(Object value, String type);
}
