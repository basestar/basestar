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

import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Accessors(fluent = true)
public class WithProperty<T, V> implements WithAccessor<T, V> {

    private final WithType<T> owner;

    private final String name;

    private final AnnotatedType annotatedType;

    private final WithField<? super T, V> field;

    private final WithMethod<? super T, V> getter;

    private final WithMethod<? super T, ?> setter;

    private final List<WithAnnotation<?>> annotations;

    public WithProperty(final WithType<T> owner, final String name, final WithField<? super T, V> field,
                        final WithMethod<? super T, V> getter, final WithMethod<? super T, ?> setter) {

        this.owner = owner;
        this.name = name;
        this.field = field;
        this.getter = getter;
        this.setter = setter;
        if(getter != null) {
            annotatedType = getter.annotatedType();
        } else if(setter != null) {
            annotatedType = setter.parameters().get(0).annotatedType();
        } else {
            annotatedType = field.annotatedType();
        }
        this.annotations = Stream.of(field, getter, setter)
                .filter(Objects::nonNull)
                .map(HasAnnotations::annotations)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public boolean canGet() {

        return getter != null || field != null;
    }

    @Override
    public boolean canSet() {

        return setter != null || (field != null && field.canSet());
    }

    @Override
    public V get(final T parent) throws IllegalAccessException, InvocationTargetException {

        if(getter != null) {
            return getter.invoke(parent);
        } else if(field != null) {
            return field.get(parent);
        } else {
            throw new IllegalAccessException();
        }
    }

    @Override
    public void set(final T parent, final V value) throws IllegalAccessException, InvocationTargetException {

        if(setter != null) {
            setter.invoke(parent, value);
        } else if(field != null) {
            field.set(parent, value);
        } else {
            throw new IllegalAccessException();
        }
    }

    @Override
    public int modifiers() {

        //FIXME
        return 0;
    }
}
