package io.basestar.schema;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestInference {

    private InstanceSchema schema;

    @BeforeEach
    void setUp() throws IOException {

        final Namespace namespace = Namespace.load(TestInference.class.getResource("inference.yml"));
        this.schema = namespace.requireInstanceSchema("Inference");
    }

    @Test
    void testConstant() {

        test("null", UseAny.DEFAULT);
        test("'test'", UseString.DEFAULT);
        test("{'x':'test'}", UseMap.from(UseString.DEFAULT));
        test("[1, 1.2]", UseArray.from(UseNumber.DEFAULT));
        test("{true}", UseSet.from(UseBoolean.DEFAULT));
    }

    @Test
    void testAdd() {

        test("string + number", UseString.DEFAULT);
        test("string + boolean", UseString.DEFAULT);
        test("number + string", UseString.DEFAULT);
        test("integer + string", UseString.DEFAULT);
        test("integer + number", UseNumber.DEFAULT);
        test("number + integer", UseNumber.DEFAULT);
        test("integer + integer", UseInteger.DEFAULT);
    }

    @Test
    void testArithmetic() {

        test("integer + number", UseNumber.DEFAULT);
        test("number / integer", UseNumber.DEFAULT);
        test("integer * integer", UseInteger.DEFAULT);
        test("integer - number", UseNumber.DEFAULT);
        test("number ** integer", UseNumber.DEFAULT);
        test("number % integer", UseNumber.DEFAULT);
        test("-number", UseNumber.DEFAULT);
    }

    @Test
    void testCoalesce() {

        test("string ?? string", UseString.DEFAULT);
        test("integer ?? number", UseNumber.DEFAULT);
        test("number ?? integer", UseNumber.DEFAULT);
        test("integer ?? integer", UseInteger.DEFAULT);
    }

    @Test
    void testIfElse() {

        test("boolean ? string : string", UseString.DEFAULT);
        test("boolean ? integer : number", UseNumber.DEFAULT);
        test("boolean ? number : integer", UseNumber.DEFAULT);
        test("boolean ? integer : integer", UseInteger.DEFAULT);
    }

    @Test
    void testBitwise() {

        test("missing | missing", UseInteger.DEFAULT);
        test("missing & missing", UseInteger.DEFAULT);
        test("missing ^ missing", UseInteger.DEFAULT);
        test("~missing", UseInteger.DEFAULT);
        test("missing << missing", UseInteger.DEFAULT);
        test("missing >> missing", UseInteger.DEFAULT);
    }

    @Test
    void testLogical() {

        test("missing && missing", UseBoolean.DEFAULT);
        test("missing || missing", UseBoolean.DEFAULT);
        test("!missing", UseBoolean.DEFAULT);
    }

    @Test
    void testCompare() {

        test("missing <=> missing", UseInteger.DEFAULT);
        test("missing == missing", UseBoolean.DEFAULT);
        test("missing != missing", UseBoolean.DEFAULT);
        test("missing < missing", UseBoolean.DEFAULT);
        test("missing <= missing", UseBoolean.DEFAULT);
        test("missing > missing", UseBoolean.DEFAULT);
        test("missing >= missing", UseBoolean.DEFAULT);
    }

    @Test
    void testMemberCall() {

        test("string.substr(1)", UseString.DEFAULT);
        test("string.isEmpty()", UseBoolean.DEFAULT);
    }

    @Test
    void testIndex() {

        test("mapString[string]", UseString.DEFAULT);
        test("setInteger[integer]", UseInteger.DEFAULT);
        test("arrayNumber[number]", UseNumber.DEFAULT);
    }

    @Test
    void testAggregate() {

        test("avg(missing)", UseNumber.DEFAULT);
        test("sum(number)", UseNumber.DEFAULT);
        test("count(true)", UseInteger.DEFAULT);
        test("min(integer)", UseInteger.DEFAULT);
        test("max(string)", UseString.DEFAULT);
        test("collectArray(string)", UseArray.from(UseString.DEFAULT));
    }

    @Test
    void testLiteral() {

        test("{\"x\": string}", UseMap.from(UseString.DEFAULT));
        test("{number}", UseSet.from(UseNumber.DEFAULT));
        test("[integer]", UseArray.from(UseInteger.DEFAULT));
    }

    @Test
    void testWith() {

        test("with(x = number) integer + x", UseNumber.DEFAULT);
    }

    @Test
    void testFor() {

        test("x for all missing of missing", UseBoolean.DEFAULT);
        test("x for any missing of missing", UseBoolean.DEFAULT);
        test("{'x': f for (k, v) of mapString}", UseMap.DEFAULT);
        test("[x for x of setInteger]", UseArray.DEFAULT);
        test("{x for x of arrayNumber}", UseSet.DEFAULT);
    }

    @Test
    void testIn() {

        test("missing in missing", UseBoolean.DEFAULT);
    }

    @Test
    void testLike() {

        test("missing like missing", UseBoolean.DEFAULT);
    }

    @Test
    void testMember() {

        test("other.other.other.string", UseString.DEFAULT);
    }

    private void test(final String expressionStr, final Use<?> expected) {

        final InferenceContext context = InferenceContext.from(schema);
        final InferenceVisitor visitor = new InferenceVisitor(context);
        final Expression expression = Expression.parseAndBind(Context.init(), expressionStr);
        final Use<?> actual = visitor.visit(expression);
        assertEquals(expected, actual);
    }
}
