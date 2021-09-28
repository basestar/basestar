package io.basestar.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.jsr380.groups.Default;
import io.basestar.schema.jsr380.groups.Fatal;
import io.basestar.schema.use.*;
import io.basestar.schema.validation.*;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import javax.validation.constraints.*;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class TestConstraints {

    @Test
    void testConstraintSyntax() throws IOException {

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
        assertTrue(constraints.contains(Constraint.of(new PatternValidation.Validator("[^\\s]+"), "with groups", ImmutableList.of(), ImmutableSet.of(Fatal.NAME))));
    }

    @SuppressWarnings("unsued")
    private static class WithJsr380 {

        @AssertFalse
        @AssertTrue
        private boolean field1;

        @Max(20)
        @Min(10)
        private boolean field2;

        @DecimalMax("20.0")
        @DecimalMin("10.0")
        private boolean field3;

        @Pattern(regexp = ".?")
        @Size(min = 10, max = 20)
        private boolean field4;
    }

    @Test
    void testFromJsr380() throws Exception {

        final Field field1 = WithJsr380.class.getDeclaredField("field1");
        final Field field2 = WithJsr380.class.getDeclaredField("field2");
        final Field field3 = WithJsr380.class.getDeclaredField("field3");
        final Field field4 = WithJsr380.class.getDeclaredField("field4");

        assertFromJsr380(new AssertValidation.Validator(Expression.parse("value")),
                UseBoolean.DEFAULT, field1.getAnnotation(AssertTrue.class));

        assertFromJsr380(new AssertValidation.Validator(Expression.parse("!value")),
                UseBoolean.DEFAULT, field1.getAnnotation(AssertFalse.class));

        assertFromJsr380(new MaxValidation.Validator(20L),
                UseInteger.DEFAULT, field2.getAnnotation(Max.class));

        assertFromJsr380(new MinValidation.Validator(10L),
                UseInteger.DEFAULT, field2.getAnnotation(Min.class));

        assertFromJsr380(new MaxValidation.Validator(20.0),
                UseNumber.DEFAULT, field3.getAnnotation(DecimalMax.class));

        assertFromJsr380(new MinValidation.Validator(10.0),
                UseNumber.DEFAULT, field3.getAnnotation(DecimalMin.class));

        assertFromJsr380(new PatternValidation.Validator(".?"),
                UseString.DEFAULT, field4.getAnnotation(Pattern.class));

        assertFromJsr380(new SizeValidation.Validator(10, 20),
                UseString.DEFAULT, field4.getAnnotation(Size.class));
    }

    private void assertFromJsr380(final Validation.Validator expected, final Use<?> use, final Annotation annot) {

        final Optional<Validation.Validator> actual = Validation.createJsr380Validator(use, annot);
        assertEquals(Optional.of(expected), actual);
        if(actual.isPresent()) {
            final Annotation result = actual.get().toJsr380(use, ImmutableMap.of());
            assertEquals(annot, result);
        }
    }

    @Test
    void testMin() {

        final Validation.Validator validator = new MinValidation.Validator(10L);
        assertTrue(validator.validate(UseInteger.DEFAULT, Context.init(), 5L));
        assertFalse(validator.validate(UseInteger.DEFAULT, Context.init(), 15L));
    }

    @Test
    void testMax() {

        final Validation.Validator validator = new MaxValidation.Validator(10L);
        assertFalse(validator.validate(UseInteger.DEFAULT, Context.init(), 5L));
        assertTrue(validator.validate(UseInteger.DEFAULT, Context.init(), 15L));
    }

    @Test
    void testSize() {

        final Validation.Validator validator = new SizeValidation.Validator(3, 5);
        assertTrue(validator.validate(UseString.DEFAULT, Context.init(), "hello"));
        assertFalse(validator.validate(UseString.DEFAULT, Context.init(), "he"));
        assertFalse(validator.validate(UseString.DEFAULT, Context.init(), "helloworld"));
        assertTrue(validator.validate(UseArray.DEFAULT, Context.init(), ImmutableList.of("a", "b", "c", "d")));
        assertFalse(validator.validate(UseArray.DEFAULT, Context.init(), ImmutableList.of("a")));
        assertFalse(validator.validate(UseArray.DEFAULT, Context.init(), ImmutableList.of("a", "b", "c", "d", "e", "f", "g")));
        assertTrue(validator.validate(UseMap.DEFAULT, Context.init(), ImmutableList.of("a", "b", "c", "d").stream().collect(Collectors.toMap(v -> v, v -> v))));
        assertFalse(validator.validate(UseMap.DEFAULT, Context.init(), ImmutableList.of("a").stream().collect(Collectors.toMap(v -> v, v -> v))));
        assertFalse(validator.validate(UseMap.DEFAULT, Context.init(), ImmutableList.of("a", "b", "c", "d", "e", "f", "g").stream().collect(Collectors.toMap(v -> v, v -> v))));
    }

    @Test
    void testPattern() {

        final Validation.Validator validator = new PatternValidation.Validator("\\w+");
        assertTrue(validator.validate(UseString.DEFAULT, Context.init(), "hello"));
        assertFalse(validator.validate(UseString.DEFAULT, Context.init(), "hello?"));
    }

    @Test
    void testSizeMessage() {

        assertEquals("size must be 1", new SizeValidation.Validator(1).defaultMessage());
        assertEquals("size must be at least 1", new SizeValidation.Validator(1, null).defaultMessage());
        assertEquals("size must be at most 2", new SizeValidation.Validator(null, 2).defaultMessage());
        assertEquals("size must be between 1 and 2", new SizeValidation.Validator(1, 2).defaultMessage());
    }

    @Test
    void testViolationSerialization() {

        final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);
        final Map<?, ?> withGroup = objectMapper.convertValue(new Constraint.Violation(Name.of("name"), "type", "message", ImmutableSet.of("group1")), Map.class);
        assertEquals(ImmutableList.of("group1"), withGroup.get("groups"));

        final Map<?, ?> defaultGroup = objectMapper.convertValue(new Constraint.Violation(Name.of("name"), "type", "message", ImmutableSet.of()), Map.class);
        assertEquals(ImmutableList.of(Default.NAME), defaultGroup.get("groups"));
    }
}
