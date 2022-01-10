package io.basestar.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.from.FromSchema;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFunctionSchema {


    @Test
    public void testReplacedDefinition() {

        final String input = "SELECT * FROM @{test}";
        final String output = FunctionSchema.getReplacedDefinition(input, ImmutableMap.of("test", new FromSchema(
                ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()
        )), v -> v.getQualifiedName().toString());

        assertEquals("SELECT * FROM test.Object", output);
    }

    @ParameterizedTest(name = "{index}: replace\"{1}\", in \"{0}\"")
    @MethodSource("dotsReplacements")
    public void shouldReplaceDotsInProvidedSearchTerm(final String input, final String searchTerm, final String expected) {
        //if dots not replaced it can match any char i.e. DOG,CONTEST would match for DOG.CONTEST
        final String output = FunctionSchema.getReplacedDefinition(
                input,
                ImmutableMap.of(searchTerm, new FromSchema(ObjectSchema.builder().build(Name.of("dog", "contestWinners")), ImmutableSet.of())),
                v -> v.getQualifiedName().toString());

        assertEquals(expected, output);
    }

    private static Stream<Arguments> dotsReplacements() {
        return Stream.of(
                Arguments.of("SELECT DOG,CONTEST FROM DOG.CONTEST;", "DOG.CONTEST", "SELECT DOG,CONTEST FROM dog.contestWinners;"),
                Arguments.of("SELECT DOG,CONTEST FROM DOG.CONTEST", "DOG.CONTEST", "SELECT DOG,CONTEST FROM dog.contestWinners"),
                Arguments.of("SELECT DOG,CONTEST FROM \"DOG\".\"CONTEST\"", "\"DOG\".\"CONTEST\"", "SELECT DOG,CONTEST FROM dog.contestWinners"),
                Arguments.of("SELECT DOG_CONTEST FROM \"DOG\".\"CONTEST\"", "\"DOG\".\"CONTEST\"", "SELECT DOG_CONTEST FROM dog.contestWinners"),
                Arguments.of("SELECT DOG_CONTEST FROM \"DOG.CONTEST\"", "DOG.CONTEST", "SELECT DOG_CONTEST FROM dog.contestWinners")
        );
    }

    @ParameterizedTest(name = "{index}: replace\"{1}\", in \"{0}\"")
    @MethodSource("replacementAtTheStart")
    public void shouldReplaceIfSearchTermIsAtTheStartOfGivenString(final String input, final String searchTerm, final String expected) {

        final String output = FunctionSchema.getReplacedDefinition(
                input,
                ImmutableMap.of(searchTerm, new FromSchema(ObjectSchema.builder().build(Name.of("dog", "contest")), ImmutableSet.of())),
                v -> v.getQualifiedName().toString());

        assertEquals(expected, output);
    }

    private static Stream<Arguments> replacementAtTheStart() {
        return Stream.of(
                Arguments.of("DOG_CONTEST(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "DOG_CONTEST", "dog.contest(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE"),
                Arguments.of(" DOG_CONTEST(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "DOG_CONTEST", " dog.contest(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE"),
                Arguments.of("\"DOG_CONTEST\"(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "\"DOG_CONTEST\"", "dog.contest(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE"),
                Arguments.of("\"DOG\".\"CONTEST\"(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "\"DOG\".\"CONTEST\"", "dog.contest(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE"),
                Arguments.of("\"DOG\".\"CONTEST\" (ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "\"DOG\".\"CONTEST\"", "dog.contest (ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE"),
                Arguments.of("\"DOG.CONTEST\" (ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE", "DOG.CONTEST", "dog.contest (ARRAY_TO_STRING(ARRAY_AGG(DISTINCT  MVW.SOURCE), ',')) AS SCORE")
        );
    }

    @ParameterizedTest(name = "{index}: replace\"{1}\", in \"{0}\"")
    @MethodSource("replacementAtTheEnd")
    public void shouldReplaceIfSearchTermIsAtTheEndOfGivenString(final String input, final String searchTerm, final String expected) {


        final String output = FunctionSchema.getReplacedDefinition(input, ImmutableMap.of(searchTerm, new FromSchema(
                ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()
        )), v -> v.getQualifiedName().toString());

        assertEquals(expected, output);
    }

    private static Stream<Arguments> replacementAtTheEnd() {
        return Stream.of(
                Arguments.of("SELECT * FROM test;", "test", "SELECT * FROM test.Object;"),
                Arguments.of("SELECT * FROM test.targets;", "test.targets", "SELECT * FROM test.Object;"),
                Arguments.of("SELECT * FROM \"test\".\"targets\";", "\"test\".\"targets\"", "SELECT * FROM test.Object;"),
                Arguments.of("SELECT * FROM test_targets", "test_targets", "SELECT * FROM test.Object"),
                Arguments.of("SELECT * FROM \"test_targets\"", "test_targets", "SELECT * FROM test.Object")
        );
    }

    @ParameterizedTest(name = "{index}: replace\"{1}\", in \"{0}\"")
    @MethodSource("replacementInTheMiddle")
    public void shouldReplaceIfSearchTermIsInTheMiddle(final String input, final String searchTerm, final String expected) {

        final String output = FunctionSchema.getReplacedDefinition(input, ImmutableMap.of(searchTerm, new FromSchema(
                ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()
        )), v -> v.getQualifiedName().toString());

        assertEquals(expected, output);
    }

    private static Stream<Arguments> replacementInTheMiddle() {
        return Stream.of(
                Arguments.of("SELECT * FROM test where result is null", "test", "SELECT * FROM test.Object where result is null"),
                Arguments.of("SELECT * FROM test_runs where result is null", "test_runs", "SELECT * FROM test.Object where result is null"),
                Arguments.of("SELECT * FROM test.runs where result is null", "test.runs", "SELECT * FROM test.Object where result is null"),
                Arguments.of("SELECT * FROM \"test\".\"runs\" where result is null", "\"test\".\"runs\"", "SELECT * FROM test.Object where result is null")
        );
    }

    @Test
    public void shouldReplaceIfMultipleDifferentMatches() {

        final String input = "SELECT first.function(second.function(someValue)) as transformed FROM test where result is null JOIN \"badTestRuns\" btr ON btr.id = id JOIN @{example_test_runs} on id JOIN badTestRuns ON id";
        final String output = FunctionSchema.getReplacedDefinition(input,
                ImmutableMap.of("test", new FromSchema(ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()),
                        "badTestRuns", new FromSchema(ObjectSchema.builder().build(Name.of("bad", "test", "runs")), ImmutableSet.of()),
                        "first.function", new FromSchema(ObjectSchema.builder().build(Name.of("first", "good")), ImmutableSet.of()),
                        "second.function", new FromSchema(ObjectSchema.builder().build(Name.of("second", "good")), ImmutableSet.of()),
                        "example_test_runs", new FromSchema(ObjectSchema.builder().build(Name.of("example_test_runs")), ImmutableSet.of())
                ),
                v -> v.getQualifiedName().toString());

        assertEquals("SELECT first.good(second.good(someValue)) as transformed FROM test.Object where result is null JOIN bad.test.runs btr ON btr.id = id JOIN example_test_runs on id JOIN bad.test.runs ON id", output);
    }
}