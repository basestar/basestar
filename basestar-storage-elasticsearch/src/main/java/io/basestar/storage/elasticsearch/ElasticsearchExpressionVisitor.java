package io.basestar.storage.elasticsearch;

/*-
 * #%L
 * basestar-storage-elasticsearch
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
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.function.In;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.util.Path;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.Objects;

public class ElasticsearchExpressionVisitor implements ExpressionVisitor.Defaulting<QueryBuilder> {

    @Override
    public QueryBuilder visitDefault(final Expression expression) {

        return null;
    }

    @Override
    public QueryBuilder visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        return visitEqImpl(lhs, rhs);
    }

    private QueryBuilder visitEqImpl(final Expression lhs, final Expression rhs) {

        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return QueryBuilders.termQuery(
                    ((PathConstant) lhs).getPath().toString(),
                    ((Constant) rhs).getValue()
            );
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return QueryBuilders.termQuery(
                    ((PathConstant) rhs).getPath().toString(),
                    ((Constant) lhs).getValue()
            );
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitGt(final Gt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return QueryBuilders.rangeQuery(((PathConstant) lhs).getPath().toString())
                    .gt(((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return QueryBuilders.rangeQuery(((PathConstant) rhs).getPath().toString())
                    .lte(((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitGte(final Gte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return QueryBuilders.rangeQuery(((PathConstant) lhs).getPath().toString())
                    .gte(((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return QueryBuilders.rangeQuery(((PathConstant) rhs).getPath().toString())
                    .lt(((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitLt(final Lt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return QueryBuilders.rangeQuery(((PathConstant) lhs).getPath().toString())
                    .lt(((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return QueryBuilders.rangeQuery(((PathConstant) rhs).getPath().toString())
                    .gte(((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitLte(final Lte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return QueryBuilders.rangeQuery(((PathConstant) lhs).getPath().toString())
                    .lte(((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return QueryBuilders.rangeQuery(((PathConstant) rhs).getPath().toString())
                    .gt(((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitNe(final Ne expression) {

        final QueryBuilder query = visitEqImpl(expression.getLhs(), expression.getRhs());
        return query == null ? null : QueryBuilders.boolQuery().mustNot(query);
    }

    @Override
    public QueryBuilder visitIn(final In expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueryBuilder visitAnd(final And expression) {

        BoolQueryBuilder result = QueryBuilders.boolQuery();
        for(final Expression term : expression.getTerms()) {
            final QueryBuilder query = term.visit(this);
            if(query != null) {
                result = result.must(query);
            }
        }
        return result;
    }

    @Override
    public QueryBuilder visitNot(final Not expression) {

        final QueryBuilder term = expression.getOperand().visit(this);
        return term == null ? null : QueryBuilders.boolQuery().mustNot(term);
    }

    @Override
    public QueryBuilder visitOr(final Or expression) {

        final QueryBuilder[] terms = expression.getTerms().stream().map(v -> v.visit(this))
                .toArray(QueryBuilder[]::new);
        // Unlike and, if any OR elements are null we can't create a condition
        if(Arrays.stream(terms).allMatch(Objects::nonNull)) {
            BoolQueryBuilder result = QueryBuilders.boolQuery();
            for(final QueryBuilder term : terms) {
                result = result.should(term);
            }
            return result;
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitForAny(final ForAny expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(rhs instanceof Of) {
            final Of of = (Of)rhs;
            if(of.getExpr() instanceof PathConstant) {
                final Path path = ((PathConstant) of.getExpr()).getPath();
                // map keys not supported
                if(of.getKey() == null) {
                    final String value = of.getValue();
                    final Expression bound = lhs.bind(Context.init(), PathTransform.move(Path.of(value), path));
                    final QueryBuilder lhsQuery = bound.visit(this);
                    return QueryBuilders.nestedQuery(path.toString(), lhsQuery, ScoreMode.Avg);
                }
            }
        }
        return null;
    }
}
