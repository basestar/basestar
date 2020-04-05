package io.basestar.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MemoryStash implements Stash {

    private final ConcurrentMap<String, byte[]> backing = new ConcurrentHashMap<>();

    private MemoryStash(final Builder builder) {

    }

    public static Builder builder() {

        return new Builder();
    }

    public static class Builder {

        public MemoryStash build() {

            return new MemoryStash(this);
        }
    }

    @Override
    public CompletableFuture<String> write(final String key, final byte[] data) {

        backing.put(key, data);
        return CompletableFuture.supplyAsync(() -> key);
    }

    @Override
    public CompletableFuture<byte[]> read(final String key) {

        return CompletableFuture.supplyAsync(() -> backing.get(key));
    }

    @Override
    public CompletableFuture<?> delete(final String key) {

        backing.remove(key);
        return CompletableFuture.supplyAsync(() -> null);
    }
}
