package io.basestar.schema.use;

/*-
 * #%L
 * basestar-schema
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

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Schema;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import io.leangen.geantyref.TypeFactory;
import io.swagger.v3.oas.models.media.ArraySchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Set Type
 *
 * <strong>Example</strong>
 * <pre>
 * type:
 *   set: string
 * </pre>
 *
 * @param <T>
 */

@Data
@Slf4j
public class UseSet<T> implements UseCollection<T, Set<T>> {

    public static UseSet<Object> DEFAULT = new UseSet<>(UseAny.DEFAULT);

    public static final String NAME = "set";

    private final Use<T> type;

    public static <T> UseSet<T> from(final Use<T> type) {

        return new UseSet<>(type);
    }

    public static UseSet<?> from(final Object config) {

        return Use.fromNestedConfig(config, (type, nestedConfig) -> new UseSet<>(type));
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitSet(this);
    }

    @SuppressWarnings("unchecked")
    public <T2> UseSet<T2> transform(final Function<Use<T>, Use<T2>> fn) {

        final Use<T2> type2 = fn.apply(type);
        if(type2 == type ) {
            return (UseSet<T2>)this;
        } else {
            return new UseSet<>(type2);
        }
    }

    @Override
    public Object toConfig(final boolean optional) {

        return ImmutableMap.of(
                Use.name(NAME, optional), type
        );
    }

    @Override
    public UseSet<?> resolve(final Schema.Resolver resolver) {

        final Use<?> resolved = type.resolve(resolver);
        if(resolved == type) {
            return this;
        } else {
            return new UseSet<>(resolved);
        }
    }

    @Override
    public Set<T> create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createSet(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.SET;
    }

    @Override
    public Type javaType(final Name name) {

        if(name.isEmpty()) {
            return TypeFactory.parameterizedClass(Set.class, type.javaType());
        } else {
            return type.javaType(name.withoutFirst());
        }
    }

    @Override
    public Set<T> defaultValue() {

        return Collections.emptySet();
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new ArraySchema().items(type.openApi(expand)).uniqueItems(true);
    }

    @Override
    public Set<T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> Set<T> deserializeAnyValue(final DataInput in) throws IOException {

        final Set<T> result = new HashSet<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            result.add(Use.deserializeAny(in));
        }
        return result;
    }

    @Override
    public Set<T> transformValues(final Set<T> value, final BiFunction<Use<T>, T, T> fn) {

        if(value != null) {
            boolean changed = false;
            final Set<T> result = new HashSet<>();
            for(final T before : value) {
                final T after = fn.apply(type, before);
                result.add(after);
                changed = changed || (before != after);
            }
            return changed ? result : value;
        } else {
            return null;
        }
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }

    @Override
    public boolean areEqual(final Set<T> a, final Set<T> b) {

        if(a == null || b == null) {
            return a == null && b == null;
        } else if(a.size() == b.size()) {
            for(final T v : a) {
                if(!b.contains(v)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString(final Set<T> value) {

        if(value == null) {
            return "null";
        } else {
            final Use<T> type = getType();
            return "{" + value.stream().map(type::toString).sorted().collect(Collectors.joining(", ")) + "}";
        }
    }
}
