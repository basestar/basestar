package io.basestar.schema;

import com.google.common.collect.ImmutableSet;
import io.basestar.schema.validation.*;
import org.junit.jupiter.api.Test;

import javax.validation.constraints.Pattern;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConstraints {

    @Test
    public void testConstraintSyntax() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("constraints.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Constrained");

        final Property property = schema.getProperties().get("name");

        final List<Constraint> constraints = property.getConstraints();
        assertTrue(constraints.contains(Constraint.of(new SizeValidation.Validator(1, 10), "full size")));
        assertTrue(constraints.contains(Constraint.of(new SizeValidation.Validator(50), "short size")));
        assertTrue(constraints.contains(Constraint.of(new AssertValidation.Validator("value != 1"), "full assert")));
        assertTrue(constraints.contains(Constraint.of(new AssertValidation.Validator("value != 2"), "short assert")));
        assertTrue(constraints.contains(Constraint.of(new MinValidation.Validator(BigDecimal.valueOf(20), true), "full min")));
        assertTrue(constraints.contains(Constraint.of(new MinValidation.Validator(BigDecimal.valueOf(10), false), "short min")));
        assertTrue(constraints.contains(Constraint.of(new MaxValidation.Validator(BigDecimal.valueOf(200), true), "full max")));
        assertTrue(constraints.contains(Constraint.of(new MaxValidation.Validator(BigDecimal.valueOf(100), false), "short max")));
        assertTrue(constraints.contains(Constraint.of(new PatternValidation.Validator("[\\w\\d]+", ImmutableSet.of(Pattern.Flag.CASE_INSENSITIVE)), "full pattern")));
        assertTrue(constraints.contains(Constraint.of(new PatternValidation.Validator("[\\w\\d]*"), "short pattern")));
        assertTrue(constraints.contains(Constraint.of(new PatternValidation.Validator("[\\w\\d]*"), "short pattern")));
    }
}
