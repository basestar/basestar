package io.basestar.secret;


import com.google.common.io.BaseEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public interface Secret {

    static Secret.Encrypted encrypted(final byte[] encrypted) {

        return new Secret.Encrypted(encrypted);
    }

    static Secret.Encrypted encrypted(final String encrypted) {

        return new Secret.Encrypted(BaseEncoding.base64().decode(encrypted));
    }

    static Secret.Plaintext plaintext(final String plaintext) {

        return new Secret.Plaintext(plaintext.getBytes(StandardCharsets.UTF_8));
    }

    static Secret.Plaintext plaintext(final byte[] plaintext) {

        return new Secret.Plaintext(plaintext);
    }

    byte[] encrypted();

    default String encryptedBase64() {

        return BaseEncoding.base64().encode(encrypted());
    }

    class Encrypted implements Secret {

        private final byte[] encrypted;

        private Encrypted(final byte[] encrypted) {

            this.encrypted = encrypted;
        }

        public byte[] encrypted() {

            return encrypted;
        }

        @Override
        public int hashCode() {

            return super.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {

            return obj instanceof Secret.Encrypted
                    && Arrays.equals(((Secret.Encrypted) obj).encrypted(), encrypted);
        }

        @Override
        public String toString() {

            return "<redacted>";
        }
    }

    class Plaintext implements Secret {

        private final byte[] plaintext;

        private Plaintext(final byte[] plaintext) {

            this.plaintext = plaintext;
        }

        public byte[] plaintext() {

            return plaintext;
        }

        @Override
        public int hashCode() {

            return super.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {

            return obj instanceof Secret.Plaintext
                    && Arrays.equals(((Secret.Plaintext) obj).plaintext(), plaintext);
        }

        @Override
        public String toString() {

            return "<redacted>";
        }

        @Override
        public byte[] encrypted() {

            throw new IllegalStateException("Will not process unencrypted secret");
        }
    }
}
