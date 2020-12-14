package io.basestar.secret;

import java.util.concurrent.CompletableFuture;

public interface SecretContext {

    static SecretContext none() {

        return new SecretContext() {
            @Override
            public CompletableFuture<Secret.Encrypted> encrypt(final Secret.Plaintext plaintext) {

                throw new IllegalStateException("Secret context not provided");
            }

            @Override
            public CompletableFuture<Secret.Plaintext> decrypt(final Secret.Encrypted encrypted) {

                throw new IllegalStateException("Secret context not provided");
            }
        };
    }

    CompletableFuture<Secret.Encrypted> encrypt(Secret.Plaintext plaintext);

    CompletableFuture<Secret.Plaintext> decrypt(Secret.Encrypted secret);
}
