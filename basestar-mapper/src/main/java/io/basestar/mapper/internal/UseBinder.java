package io.basestar.mapper.internal;

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
