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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import lombok.EqualsAndHashCode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@EqualsAndHashCode(callSuper = true)
public class Path extends AbstractPath<Path> implements Comparable<Path> {

    public static final Path EMPTY = Path.of();

    public static final char DELIMITER = '/';

    public static final char WILDCARD = '*';

    protected Path(final String ... parts) {

        super(parts);
    }

    protected Path(final Iterable<String> parts) {

        super(parts);
    }

    @Override
    protected char delimiter() {

        return DELIMITER;
    }

    @Override
    protected Path create() {

        return EMPTY;
    }

    public File toFile() {

        return new File(toString(File.pathSeparator));
    }

    public java.nio.file.Path toPath() {

        return Paths.get(toString());
    }

    public URI toUri(final String scheme, final String userInfo, final String host, final int port,
                     final String query, final String fragment) throws URISyntaxException {

        return new URI(scheme, userInfo, host, port, toString(), query, fragment);
    }

    public URI toFileUri() {

        return URI.create("file:" + toString());
    }

    @Override
    protected Path create(final List<String> parts) {

        return new Path(parts);
    }

    public static Path parse(final String str) {

        return new Path(splitter(DELIMITER).split(str));
    }

    public static Path parse(final String str, final char delimiter) {

        return new Path(splitter(delimiter).split(str));
    }

    public static Path of(final String ... parts) {

        return new Path(Arrays.asList(parts));
    }

    public static Path of(final List<String> parts) {

        return new Path(parts);
    }

    public static Path of(final AbstractPath<?> path) {

        return new Path(path.getParts());
    }

    public static Path of(final File file) {

        return parse(file.toString(), File.separatorChar);
    }

    public static Path of(final java.nio.file.Path path) {

        return parse(path.toString(), File.separatorChar);
    }

    public static Path empty() {

        return new Path(Collections.emptyList());
    }

    protected static Splitter splitter(final char delimiter) {

        return Splitter.on(delimiter).trimResults();
    }

    public Stream<Path> resolve() throws IOException {

        if(isEmpty()) {
            return Stream.of();
        }
        final List<String> unwild = new ArrayList<>();
        final List<String> wild = new ArrayList<>();
        for(final String part : getParts()) {
            if(!wild.isEmpty() || part.contains(Character.toString(WILDCARD))) {
                wild.add(part);
            } else {
                unwild.add(part);
            }
        }
        if(wild.isEmpty()) {
            return Stream.of(this);
        } else {
            final Path unwildPath = Path.of(unwild);
            final Path wildPath = Path.of(wild);
            final java.nio.file.Path systemPath = unwildPath.toPath().toAbsolutePath();
            final Path absPath = Path.of(systemPath);
            return Files.walk(systemPath)
                    .filter(Files::isRegularFile)
                    .map(Path::of)
                    .map(v -> v.withoutFirst(absPath.size()))
                    .filter(v -> v.matches(wildPath))
                    .map(unwildPath::with);
        }
    }

    public Pattern toPattern() {

        if(isEmpty()) {
            return Pattern.compile("^$");
        } else {
            final StringBuilder pattern = new StringBuilder();
            final int size = size();
            for(int i = 0; i != size; ++i) {
                final String part = get(i);
                if (part.equals("**")) {
                    pattern.append(".*");
                } else {
                    if (part.contains("*")) {
                        pattern.append(Joiner.on("[^/]*").join(Streams.stream(Splitter.on("*").split(part))
                                .map(Pattern::quote).iterator()));
                    } else {
                        pattern.append(Pattern.quote(part));
                    }
                    if(i + 1 != size) {
                        pattern.append(Pattern.quote(Character.toString(DELIMITER)));
                    }
                }
            }
            return Pattern.compile("^" + pattern.toString() + "$");
        }
    }

    public boolean matches(final Path pattern) {

        final Pattern regex = pattern.toPattern();
        return regex.matcher(toString()).matches();
    }
}
