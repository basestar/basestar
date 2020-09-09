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

import io.basestar.type.has.HasAnnotations;
import io.basestar.type.has.HasModifiers;
import io.basestar.type.has.HasName;
import io.basestar.type.has.HasType;

import java.lang.reflect.InvocationTargetException;

public interface AccessorContext extends HasName, HasAnnotations, HasModifiers, HasType {

    boolean canGet();

    boolean canSet();

    <T, V> V get(T target) throws IllegalAccessException, InvocationTargetException;

    <T, V> void set(T target, V value) throws IllegalAccessException, InvocationTargetException;

    // Create a serializable accessor
    SerializableAccessor serializableAccessor();
}
