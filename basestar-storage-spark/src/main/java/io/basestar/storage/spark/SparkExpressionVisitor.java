package io.basestar.storage.spark;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.bitwise.*;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.function.*;
import io.basestar.expression.iterate.*;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

@RequiredArgsConstructor
public class SparkExpressionVisitor implements ExpressionVisitor<Column> {

    private final Dataset<?> ds;

    @Override
    public Column visitAdd(final Add expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.plus(rhs);
    }

    @Override
    public Column visitDiv(final Div expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.divide(rhs);
    }

    @Override
    public Column visitMod(final Mod expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitMul(final Mul expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.multiply(rhs);
    }

    @Override
    public Column visitNegate(final Negate expression) {

        final Column op = visit(expression.getOperand());
        return op.unary_$minus();
    }

    @Override
    public Column visitPow(final Pow expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitSub(final Sub expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.minus(rhs);
    }

    @Override
    public Column visitBitAnd(final BitAnd expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitBitFlip(final BitNot expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitBitLsh(final BitLsh expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitBitOr(final BitOr expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitBitRsh(final BitRsh expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitBitXor(final BitXor expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitCmp(final Cmp expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitEq(final Eq expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.eqNullSafe(rhs);
    }

    @Override
    public Column visitGt(final Gt expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.gt(rhs);
    }

    @Override
    public Column visitGte(final Gte expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.geq(rhs);
    }

    @Override
    public Column visitLt(final Lt expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.lt(rhs);
    }

    @Override
    public Column visitLte(final Lte expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.leq(rhs);
    }

    @Override
    public Column visitNe(final Ne expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.eqNullSafe(rhs).unary_$bang();
    }

    @Override
    public Column visitConstant(final Constant expression) {

        return lit(expression.getValue());
    }

    @Override
    public Column visitPathConstant(final PathConstant expression) {

        return ds.col(expression.getPath().toString());
    }

    @Override
    public Column visitCoalesce(final Coalesce expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return coalesce(lhs, rhs);
    }

    @Override
    public Column visitIfElse(final IfElse expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitIn(final In expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitIndex(final Index expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLambda(final Lambda expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLambdaCall(final LambdaCall expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitMember(final Member expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitMemberCall(final MemberCall expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitStarMember(final StarMember expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitWith(final With expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitForAll(final ForAll expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitForAny(final ForAny expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitForArray(final ForArray expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitForObject(final ForObject expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitForSet(final ForSet expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitOf(final Of expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitWhere(final Where expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLiteralArray(final LiteralArray expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLiteralObject(final LiteralObject expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLiteralSet(final LiteralSet expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitAnd(final And expression) {

        Column current = null;
        for(final Expression term : expression.getTerms()) {
            if(current == null) {
                current = visit(term);
            } else {
                current = current.and(visit(term));
            }
        }
        return current == null ? lit(true) : current;
    }

    @Override
    public Column visitNot(final Not expression) {

        return visit(expression).unary_$bang();
    }

    @Override
    public Column visitOr(final Or expression) {

        Column current = null;
        for(final Expression term : expression.getTerms()) {
            if(current == null) {
                current = visit(term);
            } else {
                current = current.or(visit(term));
            }
        }
        return current == null ? lit(false) : current;
    }
}
