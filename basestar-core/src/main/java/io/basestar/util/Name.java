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

import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
public class Name extends AbstractPath<Name> implements Comparable<Name> {

    public static final Name EMPTY = Name.of();

    public static final char DELIMITER = '.';

    public static final char MULTIPLE_DELIMITER = ',';

    // FIXME: names with wildcard parts should probably be represented by another type (if at-all)
    public static final Pattern PATTERN = Pattern.compile("[\\w\\d_$\\-]+|\\*");

    protected Name(final String ... parts) {

        super(parts);
    }

    protected Name(final List<String> parts) {

        super(parts);
    }

    @Override
    protected char delimiter() {

        return DELIMITER;
    }

    @Override
    protected Name create() {

        return EMPTY;
    }

    @Override
    protected Name create(final List<String> parts) {

        validateParts(parts);
        return new Name(parts);
    }

    public static Name parseNonEmpty(final String str) {

        final Name name = parse(str);
        if(name.isEmpty()) {
            throw new IllegalStateException("Cannot be empty");
        }
        return name;
    }

    public static Name parse(final String str) {

        return new Name(splitter(DELIMITER).splitToList(str));
    }

    public static Set<Name> parseSet(final String str) {

        return splitter(MULTIPLE_DELIMITER).splitToList(str).stream().map(Name::parse)
                .collect(Collectors.toSet());
    }

    public static List<Name> parseList(final String str) {

        return splitter(MULTIPLE_DELIMITER).splitToList(str).stream().map(Name::parse)
                .collect(Collectors.toList());
    }

    public static Name of(final String ... parts) {

        validateParts(parts);
        return new Name(Arrays.asList(parts));
    }

    public static Name of(final List<String> parts) {

        validateParts(parts);
        return new Name(parts);
    }

    public static Name empty() {

        return new Name(Collections.emptyList());
    }

    public boolean equalsSingle(final String path) {

        return size() == 1 && path.equals(get(0));
    }

    protected static void validateParts(final String ... parts) {

        Arrays.stream(parts).forEach(Name::validatePart);
    }

    private static void validatePart(final String part) {

        if(!PATTERN.matcher(part).matches()) {
            throw new IllegalStateException("Names can only contain alphanumeric characters or - _ $ characters (provided: " + part + ")");
        }
    }

    protected static void validateParts(final List<String> parts) {

        parts.forEach(Name::validatePart);
    }

    private static String similarityFold(final String str) {

        return str.toLowerCase().replaceAll("[$\\-]", "_");
    }

    public boolean isSimilar(final Name other) {

        final int size = size();
        if(size == other.size()) {
            for(int i = 0; i != size; ++i) {
                final String a = similarityFold(get(i));
                final String b = similarityFold(other.get(i));
                if(!a.equals(b)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
