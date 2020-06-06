package io.basestar.type;

/*-
 * #%L
 * basestar-mapper
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

import com.google.common.base.Suppliers;
import io.basestar.type.has.HasAnnotations;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Accessors(fluent = true)
public class PropertyContext implements AccessorContext {

    private final TypeContext owner;

    private final String name;

    private final AnnotatedType annotatedType;

    private final FieldContext field;

    private final MethodContext getter;

    private final MethodContext setter;

    private final Supplier<List<AnnotationContext<?>>> annotations;

    protected PropertyContext(final TypeContext owner, final String name, final FieldContext field,
                              final MethodContext getter, final MethodContext setter) {

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
        this.annotations = Suppliers.memoize(() -> Stream.<HasAnnotations>of(field, getter, setter)
                .filter(Objects::nonNull)
                .map(HasAnnotations::annotations)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
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
    public <T, V> V get(final T target) throws IllegalAccessException, InvocationTargetException {

        if(getter != null) {
            return getter.invoke(target);
        } else if(field != null) {
            return field.get(target);
        } else {
            throw new IllegalAccessException();
        }
    }

    @Override
    public <T, V> void set(final T target, final V value) throws IllegalAccessException, InvocationTargetException {

        if(setter != null) {
            setter.invoke(target, value);
        } else if(field != null) {
            field.set(target, value);
        } else {
            throw new IllegalAccessException();
        }
    }

    @Override
    public int modifiers() {

        //FIXME
        return 0;
    }

    @Override
    public List<AnnotationContext<?>> annotations() {

        return annotations.get();
    }
}
