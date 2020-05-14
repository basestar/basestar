package io.basestar.util;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TextTest {

    @Test
    public void testWords() {

        assertEquals(ImmutableList.of("hello", "world"), Text.words("hello_world").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("hello", "world"), Text.words("hello-world").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("hello"), Text.words("hello").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("HELLO"), Text.words("HELLO").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("xyz", "Abc", "D0ef", "XYZ", "Lmn0"), Text.words("xyzAbcD0efXYZLmn0").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("Xyz", "Abc", "D0ef", "XYZ", "Lmn0"), Text.words("XyzAbcD0efXYZLmn0").collect(Collectors.toList()));
    }

    @Test
    public void testCase() {

        assertEquals("helloWorld", Text.lowerCamel("HelloWorld"));
        assertEquals("AbcAbc", Text.upperCamel("abcABC"));
    }

    @Test
    public void testPlural() {

        assertEquals("CATS", Text.plural("CAT"));
        assertEquals("trusses", Text.plural("truss"));
        assertEquals("buses", Text.plural("bus"));
        assertEquals("marshes", Text.plural("marsh"));
        assertEquals("lunches", Text.plural("lunch"));
        assertEquals("taxes", Text.plural("tax"));
        assertEquals("blitzes", Text.plural("blitz"));
        assertEquals("churches", Text.plural("church"));
//        assertEquals("gasses", Text.plural("gas"));
        assertEquals("wives", Text.plural("wife"));
        assertEquals("wolves", Text.plural("wolf"));
        assertEquals("cities", Text.plural("city"));
        assertEquals("puppies", Text.plural("puppy"));
        assertEquals("rays", Text.plural("ray"));
        assertEquals("boys", Text.plural("boy"));
        assertEquals("potatoes", Text.plural("potato"));
        assertEquals("analyses", Text.plural("analysis"));
        assertEquals("phenomena", Text.plural("phenomenon"));
    }
}
