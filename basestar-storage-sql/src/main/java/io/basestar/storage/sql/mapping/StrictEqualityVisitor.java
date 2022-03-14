package io.basestar.storage.sql.mapping;


import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.logical.And;
import io.basestar.util.Name;

import java.util.HashMap;
import java.util.Map;

public class StrictEqualityVisitor implements ExpressionVisitor.Defaulting<Map<Name, Name>> {

    @Override
    public Map<Name, Name> visitDefault(final Expression expression) {

        return ImmutableMap.of();
    }

    @Override
    public Map<Name, Name> visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), ((NameConstant) rhs).getName());
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Name> visitAnd(final And expression) {

        final Map<Name, Name> result = new HashMap<>();
        expression.getTerms().forEach(e -> result.putAll(visit(e)));
        return result;
    }
}
