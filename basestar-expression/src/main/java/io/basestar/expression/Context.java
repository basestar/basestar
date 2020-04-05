package io.basestar.expression;

import io.basestar.expression.exception.MemberNotFoundException;
import io.basestar.expression.methods.Methods;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface Context extends Serializable {

//        Context EMPTY = from(Methods.builder().defaults().build(), );

    static Context init() {

        return init(Collections.emptyMap());
    }

    static Context init(final Map<String, Object> scope) {

        return init(Methods.builder().defaults().build(), scope);
    }

    static Context init(final Methods methods) {

        return init(methods, Collections.emptyMap());
    }

    static Context init(final Methods methods, final Map<String, Object> scope) {

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
            public Object call(final Object target, final String method, final Object... args) {

                return methods.call(target, method, args);
            }
        };
    }

    static Context delegating(final Context delegate, final Map<String, Object> scope) {

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
            public Object call(final Object target, final String method, final Object... args) {

                return delegate.call(target, method, args);
            }
        };
    }

    Object get(String name);

    boolean has(String name);

    default Context with(final Map<String, Object> scope) {

        return Context.delegating(this, scope);
    }

    default Context with(final String name, final Object value) {

        return with(Collections.singletonMap(name, value));
    }

    Object call(Object target, String method, Object... args);

    default Object member(final Object target, final String member) {

//            if(target instanceof StarMap<?, ?>) {
//                return ((Map<?, ?>) target).values().stream().map(
//                        v -> member(v, member)
//                ).collect(Collectors.toList());
//            } else if(target instanceof StarCollection<?>) {
//                return ((Collection<?>)target).stream().map(
//                        v -> member(v, member)
//                ).collect(Collectors.toList());
//            } else
        if(target instanceof Map<?, ?>) {
//                if ("*".equals(member)) {
//                    return new StarMap<>((Map<?, ?>)target);
//                } else {
                return ((Map<?, ?>) target).get(member);
//                }
//            } else if(target instanceof Collection<?> && "*".equals(member)) {
//                return new StarCollection<>((Collection<?>)target);
        } else {
            try {
                final String method = "get" + member.substring(0, 1).toUpperCase() + member.substring(1);
                return call(target, method);
            } catch (final MemberNotFoundException e) {
                throw new MemberNotFoundException(target.getClass(), member);
            }
        }
    }

//        @RequiredArgsConstructor
//        class StarMap<K, V> extends AbstractMap<K, V> {
//
//            private final Map<K, V> delegate;
//
//            @Nonnull
//            @Override
//            public Set<Entry<K, V>> entrySet() {
//
//                return delegate.entrySet();
//            }
//        }
//
//        @RequiredArgsConstructor
//        class StarCollection<V> extends AbstractCollection<V> {
//
//            private final Collection<V> delegate;
//
//            @Nonnull
//            @Override
//            public Iterator<V> iterator() {
//
//                return delegate.iterator();
//            }
//
//            @Override
//            public int size() {
//
//                return delegate.size();
//            }
//        }
}
