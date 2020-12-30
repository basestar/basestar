package io.basestar.expression.function;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Set;

/**
 * If-Else
 *
 * Ternary operator
 *
 */

@Data
public class IfElse implements Expression {

    public static final String TOKEN = "?:";

    public static final int PRECEDENCE = Coalesce.PRECEDENCE + 1;

    private final Expression predicate;

    private final Expression then;

    private final Expression otherwise;

    /**
     * predicate ? then : otherwise
     *
     * @param predicate boolean Left hand operand
     * @param then expression Evaluated if true
     * @param otherwise expression Evaluated if false
     */

    public IfElse(final Expression predicate, final Expression then, final Expression otherwise) {

        this.predicate = predicate;
        this.then = then;
        this.otherwise = otherwise;
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final Expression predicate = this.predicate.bind(context, root);
        if(predicate instanceof Constant) {
            final Object value = predicate.evaluate(context);
            if(Values.isTruthy(value)) {
                return then.bind(context, root);
            } else {
                return otherwise.bind(context, root);
            }
        } else {
            final Expression then = this.then.bind(context, root);
            final Expression _else = this.otherwise.bind(context, root);
            if(predicate == this.predicate && then == this.then && _else == this.otherwise) {
                return this;
            } else {
                return new IfElse(predicate, then, _else);
            }
        }
    }

    @Override
    public Object evaluate(final Context context) {

        if(predicate.evaluatePredicate(context)) {
            return then.evaluate(context);
        } else {
            return otherwise.evaluate(context);
        }
    }

    @Override
    public Set<Name> names() {

        return ImmutableSet.<Name>builder()
                .addAll(predicate.names())
                .addAll(then.names())
                .addAll(otherwise.names())
                .build();
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public boolean isConstant(final Closure closure) {

        return predicate.isConstant(closure) && then.isConstant(closure) && otherwise.isConstant(closure);
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitIfElse(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(predicate, then, otherwise);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 3;
        return new IfElse(expressions.get(0), expressions.get(1), expressions.get(2));
    }

    @Override
    public String toString() {

        return predicate + " ? " + then + " : " + otherwise;
    }
}
