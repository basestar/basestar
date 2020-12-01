package io.basestar.util;

import com.google.common.base.Charsets;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestURLs {

    @Test
    void testAll() throws IOException {

        final String target = "target/test/urls";
        Files.createDirectories(Paths.get(target, "a/b/c"));
        Files.write(Paths.get(target, "a.txt"), "a".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(Paths.get(target, "b.yml"), "b".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(Paths.get(target, "a/b/c.txt"), "c".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(Paths.get(target, "a/b/d.yml"), "d".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);
        Files.write(Paths.get(target, "a/b/c/e.json"), "e".getBytes(Charsets.UTF_8), StandardOpenOption.CREATE);

        final URL base = new File(target).toURI().toURL();
        assertEquals(1, URLs.all(URLs.toURLUnchecked(base + "*.txt")).length);
        assertEquals(2, URLs.all(URLs.toURLUnchecked(base + "**/*.yml")).length);
        assertEquals(1, URLs.all(URLs.toURLUnchecked(base + "**/*.json")).length);
        assertEquals(1, URLs.all(URLs.toURLUnchecked(base + "*/*/*.txt")).length);
    }
}
