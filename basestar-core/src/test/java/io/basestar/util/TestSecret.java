package io.basestar.util;

import io.basestar.secret.Secret;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestSecret {

    @Test
    void testPlaintextSecret() {

        final Secret.Plaintext secret = Secret.plaintext("test");
        assertNotEquals(secret, Secret.plaintext("test2"));
        assertEquals(secret, Secret.plaintext("test"));
        assertEquals(secret.hashCode(), Secret.plaintext("test").hashCode());
        assertEquals("<redacted>", secret.toString());
        assertThrows(IllegalStateException.class, secret::encrypted);
        assertThrows(IllegalStateException.class, secret::encryptedBase64);
        assertEquals("test", secret.plaintextString());
    }

    @Test
    void testEncryptedSecret() {

        final Secret.Encrypted secret = Secret.encrypted(new byte[]{1});
        assertEquals("<redacted>", secret.toString());
        assertEquals(secret, Secret.encrypted(new byte[]{1}));
        assertNotEquals(secret, Secret.encrypted(new byte[]{2}));
        assertEquals(secret, Secret.encrypted(new byte[]{1}));
        assertEquals(secret.hashCode(), Secret.encrypted(new byte[]{1}).hashCode());
        assertEquals("AQ==", secret.encryptedBase64());
    }
}
