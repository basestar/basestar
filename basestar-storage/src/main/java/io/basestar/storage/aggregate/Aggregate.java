package io.basestar.storage.aggregate;

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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface Aggregate {

    Map<String, Factory> REGISTRY = ImmutableMap.<String, Factory>builder()
            .put(Avg.NAME, Avg::create)
            .put(Count.NAME, Count::create)
            .put(Max.NAME, Max::create)
            .put(Min.NAME, Min::create)
            .put(Sum.NAME, Sum::create)
            .build();

    <T> T visit(AggregateVisitor<T> visitor);

    // Presently only used for tests
    Object evaluate(Context context, Stream<? extends Map<String, Object>> values);

    interface Factory {

        Aggregate create(List<Expression> args);
    }

    static Factory factory(final String name) {

        return REGISTRY.get(name);
    }
}
