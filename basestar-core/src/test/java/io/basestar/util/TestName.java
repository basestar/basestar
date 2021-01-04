package io.basestar.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestName {

    @Test
    void testParse() {

        assertEquals(Name.of("x", "y", "z"), Name.parse("x.y.z"));
        assertEquals(ImmutableSet.of(Name.of("x", "y", "z"), Name.of("z", "y", "x")), Name.parseSet("x.y.z, z.y.x"));
        assertEquals(ImmutableSet.of(Name.of("x", "y", "z"), Name.of("z", "y", "x")), Name.parseSet("x.y.z", "z.y.x"));
    }

    @Test
    void testWith() {

        assertEquals(Name.of("x", "y", "z"), Name.of("x").with("y").with(Name.of("z")));
    }

    @Test
    void testHeadTail() {

        assertNull(Name.empty().first());
        assertNull(Name.empty().last());
        assertEquals(Name.empty(), Name.empty().withoutFirst());
        assertEquals(Name.empty(), Name.empty().withoutLast());

        final Name abc = Name.of("a", "b", "c");
        assertEquals("a", abc.first());
        assertEquals("c", abc.last());
        assertEquals(Name.of("b", "c"), abc.withoutFirst());
        assertEquals(Name.of("a", "b"), abc.withoutLast());
    }

    @Test
    void testGetSet() {

        final Map<String, Object> data = ImmutableMap.of("x", ImmutableMap.of("y", 500L));

        assertEquals(500L, Name.of("x", "y").get(data));

        assertEquals(ImmutableMap.of(
                "x", ImmutableMap.of("y", 500L, "z", 600L),
                "a", ImmutableMap.of("b", 1000L)
        ), Name.of("x", "z").set(Name.of("a", "b").set(data, 1000L), 600L));
    }

    @Test
    void testToString() {

        assertEquals("x.y.z", Name.of("x", "y", "z").toString());
        assertEquals("x_y_z", Name.of("x", "y", "z").toString("_"));
    }

    @Test
    void testIsParent() {

        assertTrue(Name.of("x", "y", "z").isParentOrEqual(Name.of("x", "y", "z")));
        assertTrue(Name.of("x", "y").isParentOrEqual(Name.of("x", "y", "z")));
        assertFalse(Name.of("x", "y", "z").isParent(Name.of("x", "y", "z")));
        assertFalse(Name.of("x", "y", "z").isParentOrEqual(Name.of("x", "y")));
    }

    @Test
    void testChildren() {

        final Name xyz = Name.of("x", "y", "z");
        final Name xya = Name.of("x", "y", "a");
        final Name abc = Name.of("a", "b", "c");
        final Name xab = Name.of("x", "a", "b");
        final Name x = Name.of("x");

        final Set<Name> names = ImmutableSet.of(xyz, xya, abc, xab, x);

        assertEquals(ImmutableSet.of(xyz.withoutFirst(), xya.withoutFirst(), xab.withoutFirst(), Name.of()), AbstractPath.children(names, Name.of("x")));
    }

    @Test
    void testTransform() {

        assertEquals(Name.of("X", "Y", "Z"), Name.of("x", "y", "z").toUpperCase());
        assertEquals(Name.of("x", "y", "z"), Name.of("X", "Y", "Z").toLowerCase());
    }

    @Test
    void testSimplify() {

        assertEquals(ImmutableSet.of(
                Name.of("x", "y", "z")
        ), AbstractPath.simplify(ImmutableSet.of(
                Name.of("x"),
                Name.of("x", "y"),
                Name.of("x", "y", "z")
        )));
    }

    @Test
    void testBranch() {

        assertEquals(ImmutableMap.of(
                "x", ImmutableSet.of(Name.of("y", "z"), Name.of("a", "b")),
                "a", ImmutableSet.of(Name.of("b", "c"))
        ), AbstractPath.branch(ImmutableSet.of(
                Name.of("x", "y", "z"),
                Name.of("x", "a", "b"),
                Name.of("a", "b", "c")
        )));
    }
}