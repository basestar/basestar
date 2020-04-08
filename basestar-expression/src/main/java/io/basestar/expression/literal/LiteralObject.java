package io.basestar.expression.literal;

/*-
 * #%L
 * basestar-expression
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Literal Object
 *
 * Create an object by providing keys and values
 */

@Data
public class LiteralObject implements Expression {

    public static final String TOKEN = "{}";

    public static final int PRECEDENCE = LiteralSet.PRECEDENCE + 1;

    private final Map<Expression, Expression> args;

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        boolean changed = false;
        boolean constant = true;
        final Map<Expression, Expression> args = new HashMap<>();
        for(final Map.Entry<Expression, Expression> before : this.args.entrySet()) {
            final Expression key = before.getKey().bind(context, root);
            final Expression value = before.getValue().bind(context, root);
            args.put(key, value);
            constant = constant && key instanceof Constant && value instanceof Constant;
            changed = changed || key != before.getKey() || value != before.getValue();
        }
        if(constant) {
            return new Constant(new LiteralObject(args).evaluate(context));
        } else if(changed) {
            return new LiteralObject(args);
        } else {
            return this;
        }
    }

    @Override
    public Map<String, ?> evaluate(final Context context) {

        final Map<String, Object> result = new HashMap<>();
        args.forEach((k, v) -> {
            final Object key = k.evaluate(context);
            final Object value = v.evaluate(context);
            result.put(key.toString(), value);
        });
        return result;
    }

    @Override
    public Set<Path> paths() {

        return Stream.concat(args.keySet().stream(), args.values().stream())
                .flatMap(v -> v.paths().stream())
                .collect(Collectors.toSet());
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitLiteralObject(this);
    }

    @Override
    public String toString() {

        return Values.toString(args);
    }
}
