package io.basestar.storage.leveldb;

/*-
 * #%L
 * basestar-storage-leveldb
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
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

import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

class TestLevelDBStorage extends TestStorage {
    private static final Logger log = LoggerFactory.getLogger(TestLevelDBStorage.class);

    private static final File BASEDIR = new File("target/db");

    @BeforeAll
    @SuppressWarnings("ResultOfMethodCallIgnored")
    static void beforeAll() {

        BASEDIR.mkdirs();
    }

    private static volatile DBFactory factory = null;

    @Override
    protected Storage storage(final Namespace namespace) {
        /* The structure around the 'factory' static variable is that if we ever fail at building
        a JNI-based (native) LevelDB instance, we'll continue using a Java implementation; but
        we defer this determination to the first time we actually need to build a Storage.
         */
        try {
            if (factory == null) {
                factory = JniDBFactory.factory;
            }
            return buildStorage(namespace, factory);
        } catch (final UncheckedIOException e) {
            factory = Iq80DBFactory.factory;
            log.warn("Unable to build Storage using native JNI-based interface. Will substitute a slower Java implementation", e);
            return buildStorage(namespace, factory);
        } catch (final UnsatisfiedLinkError e) {
            factory = Iq80DBFactory.factory;
            log.warn("Failed to build Storage using native JNI-based interface. Will substitute a slower Java implementation", e);
            return buildStorage(namespace, factory);
        }
    }

    private Storage buildStorage(final Namespace namespace, final DBFactory factory) {
        log.debug("Building Storage using factory of type {}", factory.getClass().getName());

        try {
            final Options options = new Options();
            options.createIfMissing(true);
            final DB db = factory.open(new File(BASEDIR, UUID.randomUUID().toString()), options);
            return LevelDBStorage.builder()
                    .db(db)
                    .build();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean supportsHistoryQuery() {

        return false;
    }

    @Override
    protected boolean supportsMultiValueIndexes() {

        return false;
    }
}
