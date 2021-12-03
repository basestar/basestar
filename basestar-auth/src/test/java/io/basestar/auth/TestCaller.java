package io.basestar.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestCaller {

    @Test
    void testDeserialize() throws IOException {

        final String json = "{\"super\":false, \"schema\":\"User\", \"id\":\"matt\"}";

        final Caller caller = new ObjectMapper().readValue(json, Caller.class);

        assertFalse(caller.isSuper());
        assertEquals(Name.of("User"), caller.getSchema());
        assertEquals("matt", caller.getId());
    }

    @Test
    void testJavaSerDeSimple() throws Exception {

        testSerDe(SimpleCaller.builder()
                .setId("x")
                .setSchema(Name.of("User"))
                .setClaims(ImmutableMap.of("sub", "x"))
                .build());
    }

    @Test
    void testJavaSerDeAnon() throws Exception {

        testSerDe(Caller.ANON);
    }

    @Test
    void testJavaSerDeSuper() throws Exception {

        testSerDe(Caller.SUPER);
    }

    @Test
    void testJavaSerDeDelegating() throws Exception {

        testSerDe(new Caller.Delegating(Caller.SUPER));
    }

    private void testSerDe(final Caller expected) throws IOException, ClassNotFoundException {

        final byte[] bytes;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(expected);
            bytes = baos.toByteArray();
        }
        final Caller actual;
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             final ObjectInputStream ois = new ObjectInputStream(bais)) {
            actual = (Caller) ois.readObject();
        }
        assertEquals(expected, actual);
    }
}
