package io.basestar.schema.secret;

public class Secret {

    private final byte[] encrypted;

    public Secret(final byte[] encrypted) {

        this.encrypted = encrypted;
    }

    public byte[] encrypted() {

        return null;
    }

    public static Secret empty() {

        return null;
    }
}
