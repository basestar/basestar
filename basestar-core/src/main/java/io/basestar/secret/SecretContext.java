package io.basestar.secret;

public interface SecretContext {

    static SecretContext none() {

        return new SecretContext() {
            @Override
            public Secret.Encrypted encrypt(final Secret.Plaintext plaintext) {

                throw new IllegalStateException("Secret context not provided");
            }

            @Override
            public Secret.Plaintext decrypt(final Secret.Encrypted encrypted) {

                throw new IllegalStateException("Secret context not provided");
            }
        };
    }

    Secret.Encrypted encrypt(Secret.Plaintext plaintext);

    Secret.Plaintext decrypt(Secret.Encrypted secret);
}
