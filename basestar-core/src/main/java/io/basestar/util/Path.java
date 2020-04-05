package io.basestar.util;

import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
public class Path extends AbstractPath<Path> {

    public static final Path EMPTY = Path.of();

    public static final char DELIMITER = '.';

    public static final char MULTIPLE_DELIMITER = ',';

    public Path(final String ... parts) {

        super(parts);
    }

    public Path(final List<String> parts) {

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

    @Override
    protected Path create(final List<String> parts) {

        return new Path(parts);
    }

    public static Path parse(final String path) {

        return new Path(splitter(DELIMITER).splitToList(path));
    }

    public static Set<Path> parseSet(final String paths) {

        return splitter(MULTIPLE_DELIMITER).splitToList(paths).stream().map(Path::parse)
                .collect(Collectors.toSet());
    }

    public static List<Path> parseList(final String paths) {

        return splitter(MULTIPLE_DELIMITER).splitToList(paths).stream().map(Path::parse)
                .collect(Collectors.toList());
    }

    public static Path of(final String ... parts) {

        return new Path(Arrays.asList(parts));
    }

    public static Path of(final List<String> parts) {

        return new Path(parts);
    }

    public static Path empty() {

        return new Path(Collections.emptyList());
    }
}
