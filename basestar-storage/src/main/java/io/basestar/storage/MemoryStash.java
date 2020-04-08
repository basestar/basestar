package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
