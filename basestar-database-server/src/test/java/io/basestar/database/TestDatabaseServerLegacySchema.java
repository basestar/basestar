package io.basestar.database;

/*-
 * #%L
 * basestar-database-server
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.auth.Caller;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.expression.exception.TypeConversionException;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.secret.Secret;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.util.Name;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@Slf4j
class TestDatabaseServerLegacySchema {

    private static final Name VALIDATION = Name.of("Validation");

    private Namespace namespace;

    private DatabaseServer database;

    private Storage storage;

    private Emitter emitter;

    private Caller caller;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        this.namespace = Namespace.load(
                TestDatabaseServerLegacySchema.class.getResource("/io/basestar/database/Validation.yml")
        );
        this.emitter = Mockito.mock(Emitter.class);
        when(emitter.emit(any(Event.class))).then(inv -> {
            log.info("Emitting {}", inv.getArgument(0, Event.class));
            return CompletableFuture.completedFuture(null);
        });
        when(emitter.emit(any(Collection.class))).then(inv -> {
            final Emitter emitter = (Emitter) inv.getMock();
            final Collection<Event> events = inv.getArgument(0, Collection.class);
            events.forEach(emitter::emit);
            return CompletableFuture.completedFuture(null);
        });
        this.storage = MemoryStorage.builder().build();
        this.database = DatabaseServer.builder()
                .namespace(namespace).storage(storage)
                .emitter(emitter).build();
        this.caller = Mockito.mock(Caller.class);
    }


    @ParameterizedTest
    @MethodSource("fieldNameProvider")
    void createSimple(String fieldName) {
        final String id = UUID.randomUUID().toString();
        Map<String, Object> data = removeFieldFromData(fieldName);

        ExecutionException executionException = assertThrows(ExecutionException.class,
                () -> database.create(caller, VALIDATION, id, data).get());

        assertTrue(executionException.getCause() instanceof ConstraintViolationException);
        System.out.println(executionException.getCause().getMessage());
        assertTrue(executionException.getCause().getMessage().contains(fieldName));

    }

    //secret creation fails if value is empty wherefore we have separate test
    @Test
    void testEmptySecret() {
        final String id = UUID.randomUUID().toString();
        Map<String, Object> data = removeFieldFromData("secret");

        ExecutionException executionException = assertThrows(ExecutionException.class,
                () -> database.create(caller, VALIDATION, id, data).get());

        assertTrue(executionException.getCause() instanceof TypeConversionException);
        System.out.println(executionException.getCause().getMessage());
        assertTrue(executionException.getCause().getMessage().contains("secret"));

    }

    @Test
    void testInnerValues() {
        final String id = UUID.randomUUID().toString();

        Map<String, Object> data = data();
        Instance object = (Instance) data.get("object");
        object.set("name", null);


        ExecutionException executionException = assertThrows(ExecutionException.class,
                () -> database.create(caller, VALIDATION, id, data).get());

        assertTrue(executionException.getCause() instanceof ConstraintViolationException);
        System.out.println(executionException.getCause().getMessage());
        assertTrue(executionException.getCause().getMessage().contains("name"));

    }

    public static Map<String, Object> data() {
        return new HashMap<String, Object>() {
            {
                put("number", 5);
                put("string", "string");
                put("integer", 5);
                put("boolean", true);
                put("secret", Secret.encrypted("notreal"));
                put("mapString", ImmutableMap.of("string", "string"));
                put("arrayNumber", ImmutableList.of(5));
                put("setNumber", ImmutableSet.of(5));
                put("struct", new Instance(ImmutableMap.of(
                        "home", "123",
                        "mobile", "456",
                        "work", "789"
                )));
                put("object", new Instance(ImmutableMap.of(
                        "name", "helena"
                )));
            }
        };
    }

    public static Map<String, Object> removeFieldFromData(String key) {
        return data().entrySet().stream()
                .filter(entry -> !key.equals(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Stream<Arguments> fieldNameProvider() {
        return Stream.of(
                Arguments.of("number"),
                Arguments.of("string"),
                Arguments.of("integer"),
                Arguments.of("boolean"),
                Arguments.of("mapString"),
                Arguments.of("arrayNumber"),
                Arguments.of("setNumber"),
                Arguments.of("struct"),
                Arguments.of("object")
        );
    }
}