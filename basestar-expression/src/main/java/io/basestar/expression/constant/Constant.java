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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class Constant implements Expression {

    public static final Constant NULL = new Constant(null);

    public static final Constant FALSE = new Constant(false);

    public static final Constant TRUE = new Constant(true);

    public static final Constant EMPTY_STRING = new Constant("");

    public static final Constant EMPTY_LIST = new Constant(ImmutableList.of());

    public static final Constant EMPTY_SET = new Constant(ImmutableSet.of());

    public static final Constant EMPTY_MAP = new Constant(ImmutableMap.of());

    private static final String TOKEN = "";

    public static final int PRECEDENCE = 0;

    private final Object value;

    @Override
    public Expression bind(final Context context, final Renaming root) {

        return this;
    }

    @Override
    public Object evaluate(final Context context) {

        return value;
    }

    @Override
    public Type type(final Context context) {

        return value == null ? Void.class : value.getClass();
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public Set<Name> names() {

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
    public List<Expression> expressions() {

        return Collections.emptyList();
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 0;
        return this;
    }

    @Override
    public String toString() {

        return Values.toExpressionString(value);
    }

    public static Expression valueOf(final Boolean value) {

        if(value == null) {
            return NULL;
        } else if(value) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    public static Expression valueOf(final String value) {

        if(value == null) {
            return NULL;
        } else if(value.isEmpty()) {
            return EMPTY_STRING;
        } else {
            return new Constant(value);
        }
    }

    public static Expression valueOf(final List<?> value) {

        if(value == null) {
            return NULL;
        } else if(value.isEmpty()) {
            return EMPTY_LIST;
        } else {
            return new Constant(value);
        }
    }

    public static Expression valueOf(final Set<?> value) {

        if(value == null) {
            return NULL;
        } else if(value.isEmpty()) {
            return EMPTY_SET;
        } else {
            return new Constant(value);
        }
    }

    public static Expression valueOf(final Map<?, ?> value) {

        if(value == null) {
            return NULL;
        } else if(value.isEmpty()) {
            return EMPTY_MAP;
        } else {
            return new Constant(value);
        }
    }

    public static Expression valueOf(final Object value) {

        if(value == null) {
            return NULL;
        } else if(value instanceof Boolean) {
            return valueOf((Boolean)value);
        } else if(value instanceof String) {
            return valueOf((String)value);
        } else if(value instanceof List) {
            return valueOf((List<?>)value);
        } else if(value instanceof Set) {
            return valueOf((Set<?>)value);
        } else if(value instanceof Map) {
            return valueOf((Map<?, ?>)value);
        } else {
            return new Constant(value);
        }
    }
}
