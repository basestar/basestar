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
import lombok.EqualsAndHashCode;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
public class Name extends AbstractPath<Name> implements Comparable<Name> {

    public static final Name EMPTY = Name.of();

    public static final char DELIMITER = '.';

    public static final char MULTIPLE_DELIMITER = ',';

    // FIXME: names with wildcard parts should probably be represented by another type (if at-all)
    public static final Pattern PATTERN = Pattern.compile("[\\w\\d_$\\-]+|\\*");

    protected Name(final String... parts) {

        super(parts);
    }

    protected Name(final Iterable<String> parts) {

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
        if (name.isEmpty()) {
            throw new IllegalStateException("Cannot be empty");
        }
        return name;
    }

    public static Name parse(final String str, final char delimiter) {

        return new Name(splitter(delimiter).split(str));
    }

    public static Name parse(final String str, final String delimiter) {

        return new Name(splitter(delimiter).split(str));
    }

    public static Name parse(final String str) {

        return parse(str, DELIMITER);
    }

    public static Set<Name> parseSet(final Iterable<String> strs) {

        return Streams.stream(strs).map(Name::parse)
                .collect(Collectors.toSet());
    }

    public static Set<Name> parseSet(final String... strs) {

        return parseSet(Arrays.asList(strs));
    }

    public static Set<Name> parseSet(final String str) {

        return parseSet(splitter(MULTIPLE_DELIMITER).split(str));
    }

    public static List<Name> parseList(final Iterable<String> strs) {

        return Streams.stream(strs).map(Name::parse)
                .collect(Collectors.toList());
    }

    public static List<Name> parseList(final String... strs) {

        return parseList(Arrays.asList(strs));
    }

    public static List<Name> parseList(final String str) {

        return parseList(splitter(MULTIPLE_DELIMITER).split(str));
    }

    public static Name of(final String... parts) {

        validateParts(parts);
        return new Name(Arrays.asList(parts));
    }

    public static Name of(final List<String> parts) {

        validateParts(parts);
        return new Name(parts);
    }

    @SuppressWarnings(Warnings.SAME_METHOD_AND_FIELD_NAMES)
    public static Name empty() {

        return new Name(Collections.emptyList());
    }

    protected static Splitter splitter(final String delimiter) {

        return Splitter.on(delimiter).omitEmptyStrings().trimResults();
    }

    protected static Splitter splitter(final char delimiter) {

        return Splitter.on(delimiter).omitEmptyStrings().trimResults();
    }

    public boolean equalsSingle(final String path) {

        return size() == 1 && path.equals(at(0));
    }

    protected static void validateParts(final String... parts) {

        Arrays.stream(parts).forEach(Name::validatePart);
    }

    private static void validatePart(final String part) {

        if (!PATTERN.matcher(part).matches()) {
            throw new IllegalStateException("Names can only contain alphanumeric characters or - _ $ characters (provided: " + part + ")");
        }
    }

    protected static void validateParts(final List<String> parts) {

        parts.forEach(Name::validatePart);
    }

    public static <S extends AbstractPath<S>> Map<String, Set<S>> branch(@Nullable final Collection<S> paths) {

        return AbstractPath.branch(paths);
    }

    public static <S extends AbstractPath<S>> Set<S> children(final Collection<S> paths, final S parent) {

        return AbstractPath.children(paths, parent);
    }
}