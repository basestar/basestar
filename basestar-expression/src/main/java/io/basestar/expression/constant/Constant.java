package io.basestar.expression.constant;

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
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;
import lombok.Data;

import java.util.Collections;
import java.util.Set;

@Data
public class Constant implements Expression {

    private static final String TOKEN = "";

    public static final int PRECEDENCE = 0;

    private final Object value;

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        return this;
    }

    @Override
    public Object evaluate(final Context context) {

        return value;
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public Set<Path> paths() {

        return Collections.emptySet();
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
    public boolean isConstant(final Set<String> closure) {

        return true;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitConstant(this);
    }

    @Override
    public String toString() {

        return Values.toString(value);
    }
}
