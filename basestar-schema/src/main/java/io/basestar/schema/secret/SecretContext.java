package io.basestar.schema.secret;

public interface SecretContext {

    static SecretContext none() {
        return null;
    }

    Secret encrypt(byte[] plaintext);

    byte[] decrypt(Secret secret);
}
