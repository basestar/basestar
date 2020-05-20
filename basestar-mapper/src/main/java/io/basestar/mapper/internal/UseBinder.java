package io.basestar.mapper.internal;

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

import io.basestar.mapper.context.TypeContext;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface UseBinder {

    static UseBinder from(final TypeContext context) {

        final Class<?> erased = context.erasedType();
        if(boolean.class.isAssignableFrom(erased) || Boolean.class.isAssignableFrom(erased)) {
            return new OfBoolean(context);
        } else if(short.class.isAssignableFrom(erased) || int.class.isAssignableFrom(erased) || long.class.isAssignableFrom(erased)
            || Short.class.isAssignableFrom(erased) || Integer.class.isAssignableFrom(erased) || Long.class.isAssignableFrom(erased)) {
            return new OfInteger(context);
        } else if(float.class.isAssignableFrom(erased) || double.class.isAssignableFrom(erased)
                || Float.class.isAssignableFrom(erased) || Double.class.isAssignableFrom(erased)) {
            return new OfNumber(context);
        } else if(String.class.isAssignableFrom(erased)) {
            return new OfString(context);
        } else if(Set.class.isAssignableFrom(erased)) {
            final TypeContext setContext = context.find(Set.class);
            final TypeContext valueType = setContext.typeParameters().get(0).type();
            return new OfSet(context, from(valueType));
        } else if(Collection.class.isAssignableFrom(erased)) {
            final TypeContext collectionContext = context.find(Collection.class);
            final TypeContext valueType = collectionContext.typeParameters().get(0).type();
            return new OfArray(context, from(valueType));
        } else if(erased.isArray()) {
            // FIXME
            throw new UnsupportedOperationException();
        } else if(Map.class.isAssignableFrom(erased)){
            final TypeContext mapContext = context.find(Map.class);
            final TypeContext valueType = mapContext.typeParameters().get(1).type();
            return new OfMap(context, from(valueType));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @RequiredArgsConstructor
    class OfBoolean implements UseBinder {

        private final TypeContext context;
    }

    @RequiredArgsConstructor
    class OfInteger implements UseBinder {

        private final TypeContext context;
    }

    @RequiredArgsConstructor
    class OfNumber implements UseBinder {

        private final TypeContext context;
    }

    @RequiredArgsConstructor
    class OfString implements UseBinder {

        private final TypeContext context;
    }

    @RequiredArgsConstructor
    class OfArray implements UseBinder {

        private final TypeContext context;

        final UseBinder value;
    }

    @RequiredArgsConstructor
    class OfSet implements UseBinder {

        private final TypeContext context;

        final UseBinder value;
    }

    @RequiredArgsConstructor
    class OfMap implements UseBinder {

        private final TypeContext context;

        final UseBinder value;
    }
}
