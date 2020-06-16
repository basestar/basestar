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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.util.Path;
import lombok.Data;

import java.util.List;
import java.util.Set;

/**
 * Member
 *
 */

@Data
public class Member implements Expression {

    public static final String TOKEN = ".";

    public static final int PRECEDENCE = 0;//MemberCall.PRECEDENCE + 1;

    private final Expression with;

    private final String member;

    /**
     * with.member
     *
     * @param with any Left hand operand
     * @param member collection Right hand operand
     */

    public Member(final Expression with, final String member) {

        this.with = with;
        this.member = member;
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        final Expression with = this.with.bind(context, root);
        // Fold x.y.z into a path constant, helps with query generation
        if(with instanceof Constant) {
            return new Constant(context.member(((Constant) with).getValue(), member));
        } else if(with instanceof PathConstant) {
            return new PathConstant(((PathConstant)with).getPath().with(member));
        } else if(with == this.with) {
            return this;
        } else {
            return new Member(with, member);
        }
    }

    @Override
    public Object evaluate(final Context context) {

        final Object with = this.with.evaluate(context);
//        if(with instanceof Map<?, ?>) {
//            return ((Map<?, ?>) with).get(member);
//        } else {
        return context.member(with, member);
//        }
    }

    @Override
    public Set<Path> paths() {

        return with.paths();
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
    public boolean isConstant(final Set<String> closure) {

        return with.isConstant(closure);
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitMember(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(with);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 1;
        return new Member(with, member);
    }

    @Override
    public String toString() {

        return with + TOKEN + member;
    }
}
