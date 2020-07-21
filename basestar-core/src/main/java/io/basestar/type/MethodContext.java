package io.basestar.type;

/*-
 * #%L
 * basestar-core
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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.basestar.type.has.*;
import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("Guava")
public class MethodContext implements HasName, HasModifiers, HasAnnotations, HasParameters, HasType {

    @Getter
    private final TypeContext owner;

    @Getter
    private final Method method;

    private final Supplier<List<AnnotationContext<?>>> annotations;

    private final Supplier<List<ParameterContext>> parameters;

    protected MethodContext(final TypeContext owner, final Method method) {

        this.owner = owner;
        this.method = method;
        this.annotations = Suppliers.memoize(() -> AnnotationContext.from(method));
        this.parameters = Suppliers.memoize(() -> ParameterContext.from(owner.annotatedType(), method));
    }

    @Override
    public String name() {

        return method.getName();
    }

    @Override
    public int modifiers() {

        return method.getModifiers();
    }

    @SuppressWarnings("unchecked")
    public <T, V> V invoke(final T target, final Object ... args) throws InvocationTargetException, IllegalAccessException {

        method.setAccessible(true);
        return (V)method.invoke(target, args);
    }

    public SerializableInvoker serializableInvoker() {

        final Class<?>[] erasedParameters = parameters.get().stream()
                .map(ParameterContext::erasedType).toArray(Class<?>[]::new);
        return serializableInvoker(method.getDeclaringClass(), name(), erasedParameters);
    }

    private static SerializableInvoker serializableInvoker(final Class<?> erasedOwner, final String name, final Class<?>[] erasedParameters) {

        return new SerializableInvoker() {

            @Override
            @SuppressWarnings("unchecked")
            public <T, V> V invoke(final T target, final Object... args) throws InvocationTargetException, IllegalAccessException {

                try {
                    final Method method = erasedOwner.getDeclaredMethod(name, erasedParameters);
                    method.setAccessible(true);
                    return (V) method.invoke(target, args);
                } catch (final NoSuchMethodException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    @Override
    public AnnotatedType annotatedType() {

        return GenericTypeReflector.getReturnType(method, owner.annotatedType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Class<V> erasedType() {

        return (Class<V>)method.getReturnType();
    }

    @Override
    public List<AnnotationContext<?>> annotations() {

        return annotations.get();
    }

    @Override
    public List<ParameterContext> parameters() {

        return parameters.get();
    }
}
