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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Text {

    private static Pattern NUMERIC = Pattern.compile("-?\\d+(\\.\\d+)?");

    private static Pattern INTEGER = Pattern.compile("-?\\d+");

    private static final Set<String> PLURAL_ES = ImmutableSet.of("s", "sh", "ch", "x", "z", "o");

    private static final Set<String> PLURAL_VES = ImmutableSet.of("f", "fe");

    private static final Set<Character> VOWELS = ImmutableSet.of('a', 'e', 'i', 'o', 'u');

    private Text() {

    }

    public static boolean isVowel(final char c) {

        return VOWELS.contains(Character.toLowerCase(c));
    }

    public static boolean isConsonant(final char c) {

        return Character.isAlphabetic(c) && !isVowel(c);
    }

    @SuppressWarnings("DuplicateExpressions")
    public static String plural(final String str) {

        if(str == null || str.isEmpty()) {
            return str;
        } else {
            final String lower = str.toLowerCase();
            if(lower.endsWith("is")) {
                return caseConcat(str.substring(0, str.length() - 2), "es");
            }
            for(final String suffix : PLURAL_ES) {
                if(lower.endsWith(suffix)) {
                    return caseConcat(str, "es");
                }
            }
            for(final String suffix : PLURAL_VES) {
                if(lower.endsWith(suffix)) {
                    return caseConcat(str.substring(0, str.length() - suffix.length()), "ves");
                }
            }
            if(lower.endsWith("y")) {
                if(lower.length() > 1) {
                    final char before = lower.charAt(str.length() - 2);
                    if(isConsonant(before)) {
                        return caseConcat(str.substring(0, str.length() - 1), "ies");
                    }
                }
            }
            if(lower.length() > 2 && lower.endsWith("on") && !lower.endsWith("ion")) {
                return caseConcat(str.substring(0, str.length() - 2), "a");
            }
            return caseConcat(str, "s");
        }
    }

    private static String caseConcat(final String a, final String b) {

        if(a == null || a.isEmpty()) {
            return b;
        } else if(a.length() == 1) {
            return a + b;
        } else if(Character.isUpperCase(a.charAt(a.length() - 1))) {
            return a + b.toUpperCase();
        } else {
            return a + b;
        }
    }

    public static String upperCamel(final String str) {

        return words(str).map(Text::ucFirst).collect(Collectors.joining(""));
    }

    public static String lowerCamel(final String str) {

        final List<String> words = words(str).collect(Collectors.toList());
        if(words.isEmpty()) {
            return "";
        } else {
            return words.get(0).toLowerCase() + words.stream().skip(1).map(Text::ucFirst).collect(Collectors.joining());
        }
    }

    public static String lowerHyphen(final String str) {

        return words(str).map(String::toLowerCase).collect(Collectors.joining("-"));
    }

    public static String lowerUnderscore(final String str) {

        return words(str).map(String::toLowerCase).collect(Collectors.joining("_"));
    }

    public static String upperHyphen(final String str) {

        return words(str).map(String::toUpperCase).collect(Collectors.joining("-"));
    }

    public static String upperUnderscore(final String str) {

        return words(str).map(String::toUpperCase).collect(Collectors.joining("_"));
    }

    public static boolean isUppercase(final String str) {

        return !str.isEmpty() && str.chars().allMatch(Character::isUpperCase);
    }

    public static String ucFirst(final String str) {

        return str.isEmpty() ? str : str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    public static Stream<String> words(final String str) {

        if(str.isEmpty()) {
            return Stream.empty();
        } else if(str.contains(" ")) {
            // Probably sentence case
            return Streams.stream(Splitter.on(Pattern.compile("[^A-Za-z0-9]+")).omitEmptyStrings().split(str));
        } else if(str.contains("-")) {
            // Probably kebab-case
            return Streams.stream(Splitter.on("-").omitEmptyStrings().split(str));
        } else if(str.contains("_")) {
            // Probably snake-case
            return Streams.stream(Splitter.on("_").omitEmptyStrings().split(str));
        } else {
            // Probably some version of camelCase
            return Arrays.stream(str.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])"));
        }
    }

    public static boolean isNumber(final String str) {

        return NUMERIC.matcher(str).matches();
    }

    public static boolean isInteger(final String str) {

        return INTEGER.matcher(str).matches();
    }

    public static Number parseNumber(final String str) {

        if(str.contains(".") || str.contains("e")) {
            return Double.parseDouble(str);
        } else {
            return Long.parseLong(str);
        }
    }
}
