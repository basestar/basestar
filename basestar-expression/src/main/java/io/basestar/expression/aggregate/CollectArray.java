package io.basestar.expression.aggregate;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.exception.InvalidAggregateException;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Experimental
 */

@Data
@AllArgsConstructor
public class CollectArray implements Aggregate {

    public static final String NAME = "collectArray";

    private final Expression input;

    public static Aggregate create(final List<Expression> args) {

        if(args.size() == 1) {
            return new CollectArray(args.get(0));
        } else {
            throw new InvalidAggregateException(NAME);
        }
    }

    @Override
    public <T> T visit(final AggregateVisitor<T> visitor) {

        return visitor.visitCollectArray(this);
    }

    @Override
    public Object evaluate(final Context context, final Stream<? extends Map<String, Object>> values) {

        return values.collect(Collectors.toList());
    }
}