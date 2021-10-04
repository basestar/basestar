package io.basestar.util;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPath {

    @Test
    void testCanonical() {

        assertEquals(Path.parse("abc/xyz/Type"), Path.parse("abc").with(Path.parse("xyz/Type")).canonical());
        assertEquals(Path.parse("xyz/abc/Type"), Path.parse("abc/xyz").with(Path.parse("../../xyz/abc/Type")).canonical());
        assertEquals(Path.parse("../src/Type"), Path.parse("../src").with(Path.parse("Type")).canonical());
    }

    @Test
    void testFiles() throws IOException {

        final Path target = Path.parse("target/test/path");
        Files.createDirectories(target.with(Path.parse("a/b/c")).toPath());

        final Path a = target.with(Path.parse("a.txt"));
        final Path b = target.with(Path.parse("b.yml"));
        final Path c = target.with(Path.parse("a/b/c.txt"));
        final Path d = target.with(Path.parse("a/b/d.yml"));
        final Path e = target.with(Path.parse("a/b/c/e.json"));

        Files.write(a.toPath(), "a".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(b.toPath(), "b".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(c.toPath(), "c".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(d.toPath(), "d".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(e.toPath(), "e".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);

        assertEquals(ImmutableSet.of(a), target.with(Path.parse("*.txt")).resolve().collect(Collectors.toSet()));
        assertEquals(ImmutableSet.of(b, d), target.with(Path.parse("**/*.yml")).resolve().collect(Collectors.toSet()));
        assertEquals(ImmutableSet.of(e), target.with(Path.parse("**/*.json")).resolve().collect(Collectors.toSet()));
        assertEquals(ImmutableSet.of(c), target.with(Path.parse("*/*/*.txt")).resolve().collect(Collectors.toSet()));
    }
}
