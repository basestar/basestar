package io.basestar.schema.secret;

public interface SecretContext {

    static SecretContext none() {

        return new SecretContext() {
            @Override
            public Secret encrypt(final byte[] plaintext) {

                throw new IllegalStateException("Secret context not provided");
            }

            @Override
            public byte[] decrypt(final Secret secret) {

                throw new IllegalStateException("Secret context not provided");
            }
        };
    }

    Secret encrypt(byte[] plaintext);

    byte[] decrypt(Secret secret);

    default EncryptingValueContext encryptingValueContext() {

        return new EncryptingValueContext(this);
    }
}
