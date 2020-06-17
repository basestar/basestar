package io.basestar.storage.query;

/*-
 * #%L
 * basestar-storage
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

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.constant.PathConstant;
import io.basestar.storage.aggregate.Aggregate;
import io.basestar.util.Path;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class AggregatesVisitor implements ExpressionVisitor.Defaulting<Expression> {

    private final Map<String, Aggregate> aggregates = new HashMap<>();

    @Override
    public Expression visitDefault(final Expression expression) {

        return expression.copy(this::visit);
    }

    @Override
    public Expression visitLambdaCall(final LambdaCall expression) {

        final Expression with = expression.getWith();
        if(with instanceof PathConstant) {
            final Path path = ((PathConstant) with).getPath();
            if(path.size() == 1) {
                final String name = path.first();
                final Aggregate.Factory factory = Aggregate.factory(name);
                if(factory != null) {
                    final List<Expression> args = expression.getArgs();
                    final Aggregate aggregate = factory.create(args);
                    final String id = "v" + System.identityHashCode(aggregate);
                    aggregates.put(id, aggregate);
                    return new PathConstant(Path.of(id));
                }
            }
        }
        return visitDefault(expression);
    }
}
