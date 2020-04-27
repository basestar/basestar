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
import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Array Type
 *
 * <strong>Example</strong>
 * <pre>
 * type:
 *   array: string
 * </pre>
 *
 * @param <T>
 */

@Data
public class UseArray<T> implements UseCollection<T, List<T>> {

    public static final String NAME = "array";

    private final Use<T> type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitArray(this);
    }

    public static UseArray<?> from(final Object config) {

        return Use.fromNestedConfig(config, (type, nestedConfig) -> new UseArray<>(type));
    }

    @Override
    public Object toJson() {

        return ImmutableMap.of(
                NAME, type
        );
    }

    @Override
    public UseArray<?> resolve(final Schema.Resolver resolver) {

        return new UseArray<>(this.type.resolve(resolver));
    }

    @Override
    public List<T> create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof Collection) {
            return ((Collection<?>) value).stream()
                    .map(v -> type.create(v, expand, suppress))
                    .collect(Collectors.toList());
        } else if (suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.ARRAY;
    }

    @Override
    public List<T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> List<T> deserializeAnyValue(final DataInput in) throws IOException {

        final List<T> result = new ArrayList<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            result.add(Use.deserializeAny(in));
        }
        return result;
    }

    @Override
    public List<T> transform(final List<T> value, final Function<T, T> fn) {

        if(value != null) {
            boolean changed = false;
            final List<T> result = new ArrayList<>();
            for(final T before : value) {
                final T after = fn.apply(before);
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
//
//    class Bound<List<T>> {
//
//    }
}
