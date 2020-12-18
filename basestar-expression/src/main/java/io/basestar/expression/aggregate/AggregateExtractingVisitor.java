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

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.constant.NameConstant;
import io.basestar.util.Name;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class AggregateExtractingVisitor implements ExpressionVisitor.Defaulting<Expression> {

    private final Map<String, Aggregate> aggregates = new HashMap<>();

    @Override
    public Expression visitDefault(final Expression expression) {

        return expression.copy(this::visit);
    }

    @Override
    public Expression visitAggregate(final Aggregate expression) {

        final String id = "_" + expression.digest();
        aggregates.put(id, expression);
        return new NameConstant(Name.of(id));
    }
}
