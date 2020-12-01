package io.basestar.util;

/*-
 * #%L
 * basestar-core
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
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TextTest {

    @Test
    void testWords() {

        assertEquals(ImmutableList.of("hello", "world"), Text.words("hello_world").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("hello", "world"), Text.words("hello-world").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("hello"), Text.words("hello").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("HELLO"), Text.words("HELLO").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("xyz", "Abc", "D0ef", "XYZ", "Lmn0"), Text.words("xyzAbcD0efXYZLmn0").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("Xyz", "Abc", "D0ef", "XYZ", "Lmn0"), Text.words("XyzAbcD0efXYZLmn0").collect(Collectors.toList()));
        assertEquals(ImmutableList.of("Sentence", "with", "hyphenated", "thing"), Text.words("Sentence, with hyphenated-thing.").collect(Collectors.toList()));
    }

    @Test
    void testCase() {

        assertEquals("helloWorld", Text.lowerCamel("HelloWorld"));
        assertEquals("AbcAbc", Text.upperCamel("abcABC"));
    }

    @Test
    void testPlural() {

        assertEquals("CATS", Text.plural("CAT"));
        assertEquals("trusses", Text.plural("truss"));
        assertEquals("buses", Text.plural("bus"));
        assertEquals("marshes", Text.plural("marsh"));
        assertEquals("lunches", Text.plural("lunch"));
        assertEquals("taxes", Text.plural("tax"));
        assertEquals("blitzes", Text.plural("blitz"));
        assertEquals("churches", Text.plural("church"));
        assertEquals("wives", Text.plural("wife"));
        assertEquals("wolves", Text.plural("wolf"));
        assertEquals("cities", Text.plural("city"));
        assertEquals("puppies", Text.plural("puppy"));
        assertEquals("rays", Text.plural("ray"));
        assertEquals("boys", Text.plural("boy"));
        assertEquals("potatoes", Text.plural("potato"));
        assertEquals("analyses", Text.plural("analysis"));
        assertEquals("phenomena", Text.plural("phenomenon"));
        assertEquals("organizations", Text.plural("organization"));
        assertEquals("octopuses", Text.plural("octopus"));
    }
}
