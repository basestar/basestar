package io.basestar.schema;

import com.google.common.collect.ImmutableList;
import io.basestar.expression.Expression;
import io.basestar.schema.validator.ExpressionValidator;
import io.basestar.schema.validator.RangeValidator;
import io.basestar.schema.validator.RegexValidator;
import io.basestar.schema.validator.SizeValidator;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestConstraints {

    @Test
    public void testConstraintSyntax() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("constraints.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Constrained");

        final Property property = schema.getProperties().get("name");

        final Constraint longSize = property.getConstraints().get("longSize");
        assertEquals(ImmutableList.of(SizeValidator.builder().setMin(1L).setMax(10L).build()), longSize.getValidators());

        final Constraint exactSize = property.getConstraints().get("exactSize");
        assertEquals(ImmutableList.of(SizeValidator.builder().setMin(50L).setMax(50L).build()), exactSize.getValidators());

        final Constraint longRegex = property.getConstraints().get("longRegex");
        assertEquals(ImmutableList.of(RegexValidator.builder().setPattern("[\\w\\d]+").build()), longRegex.getValidators());

        final Constraint shortRegex = property.getConstraints().get("shortRegex");
        assertEquals(ImmutableList.of(RegexValidator.builder().setPattern("[\\w\\d]*").build()), shortRegex.getValidators());

        final Constraint longRange = property.getConstraints().get("longRange");
        assertEquals(ImmutableList.of(RangeValidator.builder().setLt(3).setGt(1).build()), longRange.getValidators());

        final Constraint custom = property.getConstraints().get("custom");
        assertEquals(ImmutableList.of(ExpressionValidator.from(Expression.parse("false"))), custom.getValidators());
    }
}
