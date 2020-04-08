package io.basestar.mapper.type;

/*-
 * #%L
 * basestar-mapper
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

import io.leangen.geantyref.GenericTypeReflector;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class WithMethod<T, V> implements HasName, HasModifiers, HasAnnotations, HasParameters, HasType<V> {

    private final WithType<T> owner;

    private final Method method;

    private final AnnotatedType annotatedType;

    private final List<WithParameter<?>> parameters;

    private final List<WithAnnotation<?>> annotations;

    protected WithMethod(final WithType<T> owner, final Method method) {

        this.owner = owner;
        this.method = method;
        this.annotatedType = GenericTypeReflector.getReturnType(method, owner.annotatedType());
        this.parameters = WithParameter.from(owner.annotatedType(), method);
        this.annotations = WithAnnotation.from(method);
    }

    @Override
    public String name() {

        return method.getName();
    }

    @Override
    public int modifiers() {

        return method.getModifiers();
    }

    public V invoke(final T parent, final Object ... args) throws InvocationTargetException, IllegalAccessException {

        return erasedType().cast(method.invoke(parent, args));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<V> erasedType() {

        return (Class<V>)method.getReturnType();
    }
}
