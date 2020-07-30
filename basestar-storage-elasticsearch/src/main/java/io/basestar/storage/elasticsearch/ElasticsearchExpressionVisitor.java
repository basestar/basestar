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
import io.basestar.expression.Renaming;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.In;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.text.Like;
import io.basestar.storage.elasticsearch.mapping.FieldType;
import io.basestar.util.Name;
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

        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return eq(((NameConstant) lhs).getName(), ((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return eq(((NameConstant) rhs).getName(), ((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    private QueryBuilder nest(final Name name, QueryBuilder query) {

        if(name.size() == 1) {
            return query;
        } else {
            for(int i = 0; i != name.size() - 1; ++i) {
                final Name nestedName = name.range(0, name.size() - (i + 1));
                query = QueryBuilders.nestedQuery(nestedName.toString(), query, ScoreMode.Avg);
            }
            return query;
        }
    }

    private QueryBuilder eq(final Name name, final Object value) {

        return nest(name, QueryBuilders.termQuery(name.toString(), value));
    }

    private QueryBuilder lt(final Name name, final Object value) {

        return nest(name, QueryBuilders.rangeQuery(name.toString()).lt(value));
    }

    private QueryBuilder gt(final Name name, final Object value) {

        return nest(name, QueryBuilders.rangeQuery(name.toString()).gt(value));
    }

    private QueryBuilder lte(final Name name, final Object value) {

        return nest(name, QueryBuilders.rangeQuery(name.toString()).lte(value));
    }

    private QueryBuilder gte(final Name name, final Object value) {

        return nest(name, QueryBuilders.rangeQuery(name.toString()).gte(value));
    }

    @Override
    public QueryBuilder visitGt(final Gt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return gt(((NameConstant) lhs).getName(), ((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return lte(((NameConstant) rhs).getName(), ((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitGte(final Gte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return gte(((NameConstant) lhs).getName(), ((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return lt(((NameConstant) rhs).getName(), ((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitLt(final Lt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return lt(((NameConstant) lhs).getName(), ((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return gte(((NameConstant) rhs).getName(), ((Constant) lhs).getValue());
        } else {
            return null;
        }
    }

    @Override
    public QueryBuilder visitLte(final Lte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return lte(((NameConstant) lhs).getName(), ((Constant) rhs).getValue());
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return gt(((NameConstant) rhs).getName(), ((Constant) lhs).getValue());
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

        // FIXME
        return visitEqImpl(expression.getLhs(), expression.getRhs());
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
            if(of.getExpr() instanceof NameConstant) {
                final Name name = ((NameConstant) of.getExpr()).getName();
                // map keys not supported
                if(of.getKey() == null) {
                    final String value = of.getValue();
                    final Expression bound = lhs.bind(Context.init(), Renaming.move(Name.of(value), name));
                    final QueryBuilder lhsQuery = bound.visit(this);
                    return QueryBuilders.nestedQuery(name.toString(), lhsQuery, ScoreMode.Avg);
                }
            }
        }
        return null;
    }

    @Override
    public QueryBuilder visitLike(final Like expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            final Name name = ((NameConstant) lhs).getName();
            final String match = Objects.toString(((Constant) rhs).getValue());
            final Like.Matcher matcher = Like.Matcher.Dialect.DEFAULT.parse(match);
            final String wildcard = Like.Matcher.Dialect.LUCENE.toString(matcher);
            return nest(name, QueryBuilders.wildcardQuery(name.toString() + FieldType.keywordSuffix(expression.isCaseSensitive()), wildcard));
        } else {
            return null;
        }
    }
}
