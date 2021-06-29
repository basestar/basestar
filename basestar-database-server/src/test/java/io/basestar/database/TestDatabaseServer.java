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
import io.basestar.auth.exception.PermissionDeniedException;
import io.basestar.database.event.*;
import io.basestar.database.exception.DatabaseReadonlyException;
import io.basestar.database.options.*;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.logical.Or;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.schema.util.Ref;
import io.basestar.secret.Secret;
import io.basestar.secret.SecretContext;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.exception.UnsupportedWriteException;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
class TestDatabaseServer {

    private static final Name SIMPLE = Name.of("Simple");

    private static final Name INDEXED = Name.of("Indexed");

    private static final Name MULTI_INDEXED = Name.of("MultiIndexed");

    private static final Name MAP_MULTI_INDEXED = Name.of("MapMultiIndexed");

    private static final Name REF_SOURCE = Name.of("RefSource");

    private static final Name REF_TARGET = Name.of("RefTarget");

    private static final Name SIMPLE_PERMS = Name.of("SimplePerms");

    private static final Name CUSTOM_ID = Name.of("CustomId");

    private static final Name ANIMAL = Name.of("Animal");

    private static final Name TEAM = Name.of("Team");

    private static final Name TEAM_MEMBER = Name.of("TeamMember");

    private static final Name CAT = Name.of("Cat");

    private static final Name DOG = Name.of("Dog");

    private static final Name KENNEL = Name.of("Kennel");

    private static final Name USER = Name.of("User");

    private static final Name VISIBILITY = Name.of("Visibility");

    private static final Name TRANSIENT = Name.of("Transient");

    private static final Name WITH_ENUM = Name.of("WithEnum");

    private static final Name READONLY = Name.of("Readonly");

    private static final Name WITH_VERSIONED_REF = Name.of("WithVersionedRef");

    private Namespace namespace;

    private DatabaseServer database;

    private Storage storage;

    private Emitter emitter;

    private Caller caller;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {

        this.namespace = Namespace.load(
                TestDatabaseServer.class.getResource("/io/basestar/database/schema.json"),
                TestDatabaseServer.class.getResource("/io/basestar/database/Team.yml")
        );
        this.emitter = Mockito.mock(Emitter.class);
        when(emitter.emit(any(Event.class))).then(inv -> {
            log.info("Emitting {}", inv.getArgumentAt(0, Event.class));
            return CompletableFuture.completedFuture(null);
        });
        when(emitter.emit(any(Collection.class))).then(inv -> {
            final Emitter emitter = (Emitter)inv.getMock();
            inv.getArgumentAt(0, Collection.class).forEach(event -> emitter.emit((Event)event));
            return CompletableFuture.completedFuture(null);
        });
        this.storage = MemoryStorage.builder().build();
        final SecretContext secretContext = new SecretContext() {
            @Override
            public CompletableFuture<Secret.Encrypted> encrypt(final Secret.Plaintext plaintext) {

                return CompletableFuture.completedFuture(Secret.encrypted(plaintext.plaintext()));
            }

            @Override
            public CompletableFuture<Secret.Plaintext> decrypt(final Secret.Encrypted encrypted) {

                return CompletableFuture.completedFuture(Secret.plaintext(encrypted.encrypted()));
            }
        };
        this.database = DatabaseServer.builder()
                .namespace(namespace).storage(storage)
                .emitter(emitter).build();
        this.caller = Mockito.mock(Caller.class);
    }

    private static void assertObject(final Name schema, final String id, final long version, final Map<String, Object> data, final Map<String, Object> object) {

        assertEquals(schema, Instance.getSchema(object));
        assertEquals(id, Instance.getId(object));
        assertEquals((Long)version, Instance.getVersion(object));
        assertNotNull(Instance.getHash(object));
        assertNotNull(Instance.getCreated(object));
        assertNotNull(Instance.getUpdated(object));
        data.forEach((k, v) -> assertEquals(v, data.get(k)));
    }

    @Test
    void createSimple() throws Exception {

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of(
                "string", "test",
                "number", 1,
                "boolean", true,
                "array", ImmutableList.of("a"),
                "map", ImmutableMap.of("a", "b")
        );

        final Map<String, Object> create = database.create(caller, SIMPLE, id, data).get();
        assertObject(SIMPLE, id, 1, data, create);

        final Map<String, Object> read = database.read(caller, SIMPLE, id).get();
        assertEquals(create, read);

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, 1L).get();
        assertEquals(read, version1);

        verify(emitter, times(1))
                .emit(ObjectCreatedEvent.of(SIMPLE, id, create));
    }

    @Test
    void updateSimple() throws Exception {

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data1 = ImmutableMap.of(
                "string", "test",
                "number", 1,
                "boolean", true,
                "array", ImmutableList.of("a"),
                "map", ImmutableMap.of("a", "b")
        );

        final Map<String, Object> create = database.create(caller, SIMPLE, id, data1).get();

        final Map<String, Object> data2 = ImmutableMap.of(
                "string", "test2",
                "number", 2,
                "boolean", false,
                "array", ImmutableList.of("b"),
                "map", ImmutableMap.of("c", "d")
        );

        final Map<String, Object> update = database.update(caller, SIMPLE, id, 1L, data2).get();
        assertObject(SIMPLE, id, 2, data2, update);

        final Map<String, Object> read = database.read(caller, SIMPLE, id).get();
        assertEquals(update, read);

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, 1L).get();
        assertEquals(create, version1);

        final Map<String, Object> version2 = database.read(caller, SIMPLE, id, 2L).get();
        assertEquals(read, version2);

        verify(emitter, times(1))
                .emit(ObjectUpdatedEvent.of(SIMPLE, id, 1L, create, update));
    }

    @Test
    void deleteSimple() throws Exception {

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of();
        final Map<String, Object> create = database.create(caller, SIMPLE, id, data).get();

        final Instance delete = database.delete(caller, SIMPLE, id, 1L).get();
        assertNull(delete);

        final Map<String, Object> read = database.read(caller, SIMPLE, id).get();
        assertNull(read);

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, 1L).get();
        assertNull(version1);

        verify(emitter, times(1))
                .emit(ObjectDeletedEvent.of(SIMPLE, id, 1L, create));
    }

    @Test
    void createIndexed() throws Exception {

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> dataA = ImmutableMap.of(
                "value", "a"
        );

        final Map<String, Object> createA = database.create(caller, INDEXED, idA, dataA).get();
        assertObject(INDEXED, idA, 1, dataA, createA);

        final Map<String, Object> readA = database.read(caller, INDEXED, idA).get();
        assertEquals(createA, readA);

        final String idB = UUID.randomUUID().toString();
        final Map<String, Object> dataB = ImmutableMap.of(
                "value", "b"
        );

        final Map<String, Object> createB = database.create(caller, INDEXED, idB, dataB).get();
        assertObject(INDEXED, idB, 1, dataB, createB);

        final Map<String, Object> readB = database.read(caller, INDEXED, idB).get();
        assertEquals(createB, readB);

        final Page<Instance> queryA = database.query(caller, INDEXED, Expression.parse("value == 'a'")).get();
        assertEquals(1, queryA.size());
        assertEquals(readA, queryA.get(0));

        final Page<Instance> queryB = database.query(caller, INDEXED, Expression.parse("value == 'b'")).get();
        assertEquals(1, queryB.size());
        assertEquals(readB, queryB.get(0));
    }

    @Test
    void createMultiIndexed() throws Exception {

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> dataA = ImmutableMap.of(
                "value", ImmutableList.of(
                        "a", "b", "c"
                )
        );

        final Map<String, Object> createA = database.create(caller, MULTI_INDEXED, idA, dataA).get();
        assertObject(MULTI_INDEXED, idA, 1, dataA, createA);

        final Map<String, Object> readA = database.read(caller, MULTI_INDEXED, idA).get();
        assertEquals(createA, readA);

        final Page<Instance> queryA = database.query(caller, MULTI_INDEXED, Expression.parse("v == 'a' for any v of value")).get();
        assertEquals(1, queryA.size());
        assertEquals(readA, queryA.get(0));
    }

    @Test
    void createMapMultiIndexed() throws Exception {

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> dataA = ImmutableMap.of(
                "value", ImmutableMap.of(
                        "x", ImmutableMap.of("key", "a"),
                        "y", ImmutableMap.of("key", "b"),
                        "z", ImmutableMap.of("key", "c")
                )
        );

        final Map<String, Object> createA = database.create(caller, MAP_MULTI_INDEXED, idA, dataA).get();
        assertObject(MAP_MULTI_INDEXED, idA, 1, dataA, createA);

        final Map<String, Object> readA = database.read(caller, MAP_MULTI_INDEXED, idA).get();
        assertEquals(createA, readA);

        final Page<Instance> queryA = database.query(caller, MAP_MULTI_INDEXED, Expression.parse("v.key == 'a' for any v of value")).get();
        assertEquals(1, queryA.size());
        assertEquals(readA, queryA.get(0));
    }

    @Test
    void createRef() throws Exception {

        final String refA = UUID.randomUUID().toString();
        final Map<String, Object> createRefA = database.create(caller, REF_TARGET, refA, ImmutableMap.of(
                "value", "test"
        )).get();

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> dataA = ImmutableMap.of(
                "target", ImmutableMap.of(
                    "id", refA
                )
        );

        final Map<String, Object> createA = database.create(caller, REF_SOURCE, idA, dataA).get();
        assertObject(REF_SOURCE, idA, 1, dataA, createA);

        final Map<String, Object> readA = database.read(caller, ReadOptions.builder().setSchema(REF_SOURCE)
                .setId(idA).setExpand(Name.parseSet("target")).build()).get();
        assertEquals(createRefA, readA.get("target"));

        final Page<Instance> linkA = database.queryLink(caller, REF_TARGET, refA, "sources").get();
        assertEquals(1, linkA.size());
        assertEquals(createA, linkA.get(0));

        final Map<String, Object> expandLinkA = database.read(caller, ReadOptions.builder().setSchema(REF_TARGET)
                .setId(refA).setExpand(Name.parseSet("sources,source")).build()).get();
        final Page<?> sources = (Page<?>)expandLinkA.get("sources");
        final Object source = expandLinkA.get("source");
        assertEquals(1, sources.size());
        assertEquals(createA, sources.get(0));
        assertEquals(createA, source);

        final Page<Instance> expandQuery = database.query(caller, QueryOptions.builder()
                .setSchema(REF_SOURCE)
                .setExpression(Expression.parse("target.id == \"" + refA + "\""))
                .setExpand(ImmutableSet.of(Name.of("target")))
                .build()).get();
        assertEquals(1, expandQuery.size());
        final Instance queryTarget = expandQuery.get(0).get("target", Instance.class);
        assertNotNull(queryTarget);
        assertNotNull(queryTarget.getHash());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nestedRef() throws Exception {

        final String idA = "a";
        final Map<String, Object> createRefA = database.create(caller, REF_TARGET, idA, ImmutableMap.of(
                "value", "a"
        )).get();

        final String idB = "b";
        final Map<String, Object> createRefB = database.create(caller, CreateOptions.builder().setSchema(REF_TARGET)
                .setId(idB).setData(ImmutableMap.of(
                "value", "b",
                "target", ImmutableMap.of(
                        "id", idA
                )
        )).setExpand(Name.parseSet("target")).build()).get();
        // Check reading refs doesn't wipe properties
        assertNotNull(createRefB.get("value"));
        assertEquals(createRefA, Instance.without((Map<String, Object>)createRefB.get("target"), Reserved.META));

        //System.err.println(Path.parseSet("target.target"));

        final String idC = UUID.randomUUID().toString();
        final Map<String, Object> createRefC = database.create(caller, CreateOptions.builder().setSchema(REF_TARGET)
                .setId(idC).setData(ImmutableMap.of(
                "value", "c",
                "target", ImmutableMap.of(
                        "id", idB
                )
        )).setExpand(Name.parseSet("target.target")).build()).get();
        assertEquals(createRefB, Instance.without((Map<String, Object>)createRefC.get("target"), Reserved.META));
    }

    @Test
    void missingRefNotNull() throws Exception {

        final String missing = UUID.randomUUID().toString();
        final String refA = UUID.randomUUID().toString();
        final Map<String, Object> createRefA = database.create(caller, CreateOptions.builder().setSchema(REF_SOURCE)
                .setId(refA).setData(ImmutableMap.of(
                "value", "test",
                "target", ImmutableMap.of(
                        "id", missing
                )
        )).setExpand(Name.parseSet("target")).build()).get();
        @SuppressWarnings("unchecked")
        final Map<String, Object> target = (Map<String, Object>)createRefA.get("target");
        assertNotNull(target);
//        assertEquals(REF_TARGET, Instance.getSchema(target));
        assertEquals(missing, Instance.getId(target));
    }

    @Test
    void simplePerms() throws Exception {

        when(caller.getSchema()).thenReturn(USER);
        when(caller.getId()).thenReturn("test");

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> createRefA = database.create(caller, SIMPLE_PERMS, idA, ImmutableMap.of(
                "owner", ImmutableMap.of(
                        "id", "test"
                )
        )).get();
        assertNotNull(createRefA);

        final String idB = UUID.randomUUID().toString();

        assertThrows(PermissionDeniedException.class, cause(() ->
            database.create(caller, SIMPLE_PERMS, idB, ImmutableMap.of(
                    "owner", ImmutableMap.of(
                            "id", "test2"
                    )
            )).get()));
    }

    @Test
    void customId() throws Exception {

        final Map<String, Object> createA = database.create(Caller.SUPER, CUSTOM_ID, ImmutableMap.of(
                "x", "x"
        )).get();
        assertNotNull(createA);
        assertEquals("custom:x", Instance.getId(createA));
    }

    @Test
    @SuppressWarnings("unchecked")
    void polymorphicCreate() {

        when(caller.getSchema()).thenReturn(USER);
        when(caller.getId()).thenReturn("test");

        final String idA = UUID.randomUUID().toString();
        database.create(caller, CAT, idA, ImmutableMap.of(
                "name", "Pippa",
                "breed", "Bengal"
        )).join();

        final String idB = UUID.randomUUID().toString();
        database.create(caller, DOG, idB, ImmutableMap.of(
                "name", "Brian",
                "breed", "Labrador"
        )).join();

        final Instance readA = database.read(caller, ANIMAL, idA).join();
        assertEquals(CAT, readA.getSchema());
        assertEquals("Bengal", readA.get("breed"));

        final Instance readB = database.read(caller, ANIMAL, idB).join();
        assertEquals(DOG, readB.getSchema());
        assertEquals("Labrador", readB.get("breed"));

        final Page<Instance> queryA = database.query(caller, ANIMAL, Expression.parse("name == 'Pippa'")).join();
        assertEquals(1, queryA.size());
        assertEquals(CAT, queryA.get(0).getSchema());
        assertEquals("Bengal", queryA.get(0).get("breed"));

        final Page<Instance> queryB = database.query(caller, ANIMAL, Expression.parse("class == 'Mammal'")).join();
        assertEquals(2, queryB.size());

        final Page<Instance> queryC = database.query(caller, CAT, Expression.parse("class == 'Mammal'")).join();
        assertEquals(1, queryC.size());

        final String idC = UUID.randomUUID().toString();
        database.create(caller, KENNEL, idC, ImmutableMap.of(
                "residents", ImmutableSet.of(
                        ImmutableMap.of(
                                "schema", "Animal",
                                "id", idA
                        ),
                        ImmutableMap.of(
                                "schema", "Dog",
                                "id", idB
                        )
                )
        )).join();

        final Set<Name> expand = ImmutableSet.of(Name.of("residents"));
        final Instance readC = database.read(caller, ReadOptions.builder().setSchema(KENNEL)
                .setId(idC).setExpand(expand).build()).join();
        final Collection<Map<String, Object>> residents = (Collection<Map<String, Object>>)readC.get("residents");
        assertTrue(residents.stream().allMatch(v -> v.get("breed") != null));
    }

    @Test
    void batch() {

        final Map<String, Instance> results = database.batch(caller, BatchOptions.builder()
                .putAction("a", CreateOptions.builder()
                        .setSchema(SIMPLE)
                        .setExpressions(ImmutableMap.of(
                                "string", Expression.parse("batch.c.setId")
                        ))
                        .build())
                .putAction("b", CreateOptions.builder()
                        .setSchema(SIMPLE)
                        .setData(ImmutableMap.of(
                                "string", "b"
                        ))
                        .setExpressions(ImmutableMap.of(
                                "array", Expression.parse("[batch.a.setId]")
                        ))
                        .build())
                .putAction("c", CreateOptions.builder()
                        .setSchema(SIMPLE)
                        .setData(ImmutableMap.of(
                                "string", "a"
                        ))
                        .build())
                .build()).join();

        log.debug("Batch results {}", results);
    }

    @Test
    void advancedPerms() throws Exception {

        when(caller.getSchema()).thenReturn(USER);
        when(caller.getId()).thenReturn("test");

        final Map<String, Instance> ok = database.batch(caller, BatchOptions.builder()
                .putAction("team", CreateOptions.builder()
                        .setSchema(TEAM)
                        .setId("t1")
                        .build())
                .putAction("member", CreateOptions.builder()
                        .setSchema(TEAM_MEMBER)
                        .setData(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "test"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();
        assertEquals(2, ok.size());

        assertThrows(PermissionDeniedException.class, cause(() ->
                database.batch(caller, BatchOptions.builder()
                        .putAction("team", CreateOptions.builder()
                                .setSchema(TEAM)
                                .setId("t2")
                                .build())
                        .putAction("member", CreateOptions.builder()
                                .setSchema(TEAM_MEMBER)
                                .setData(ImmutableMap.of(
                                        "user", ImmutableMap.of("id", "test"),
                                        "team", ImmutableMap.of("id", "t2"),
                                        "role", "owner",
                                        "accepted", false
                                )).build())
                        .build()).get()));
    }

    @Test
    void visibility() throws Exception {

        final Instance createA = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(VISIBILITY)
                .setData(ImmutableMap.of(
                        "x", 2
                ))
                .build()).get();
        assertNull(createA.get("x"));

        final Instance readA = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(VISIBILITY)
                .setId(createA.getId())
                .build()).get();
        assertNull(readA.get("x"));

        final Instance createB = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(VISIBILITY)
                .setData(ImmutableMap.of(
                        "x", 20
                ))
                .build()).get();
        assertNotNull(createB.get("x"));

        final Instance readB = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(VISIBILITY)
                .setId(createB.getId())
                .build()).get();
        assertNotNull(readB.get("x"));
    }

    @Test
    void transients() throws Exception {

        final Instance createA = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(TRANSIENT)
                .setData(ImmutableMap.of(
                        "name", "test"
                ))
                .build()).get();

        final Instance createB = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(TRANSIENT)
                .setData(ImmutableMap.of(
                        "refs", ImmutableList.of(createA)
                ))
                .setExpand(ImmutableSet.of(Name.of("names")))
                .build()).get();
        assertEquals(ImmutableList.of("test"), createB.get("names"));

        final Instance readB = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(TRANSIENT)
                .setId(createB.getId())
                .setExpand(ImmutableSet.of(Name.of("names")))
                .build()).get();
        assertEquals(ImmutableList.of("test"), readB.get("names"));
    }

    @Test
    void expand() throws Exception {

        final Map<String, Instance> ok = database.batch(Caller.SUPER, BatchOptions.builder()
                .putAction("team", CreateOptions.builder()
                        .setSchema(TEAM)
                        .setId("t1")
                        .build())
                .putAction("user", CreateOptions.builder()
                        .setSchema(USER)
                        .setId("u1")
                        .build())
                .putAction("member", CreateOptions.builder()
                        .setSchema(TEAM_MEMBER)
                        .setData(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "u1"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();
        assertEquals(3, ok.size());

        final Instance member = ok.get("member");
        assertNotNull(member);

        database.onObjectCreated(ObjectCreatedEvent.of(TEAM, "t1", ok.get("team"))).join();

        final RefQueryEvent queryEvent = RefQueryEvent.of(Ref.of(TEAM, "t1"), TEAM_MEMBER, Expression.parse("team.id == 't1' || team.parent.id == 't1'").bind(Context.init()));

        final ArgumentCaptor<Event> queryCaptor = ArgumentCaptor.forClass(Event.class);
        verify(emitter, times(4)).emit(queryCaptor.capture());
        assertTrue(queryCaptor.getAllValues().contains(queryEvent));

        database.onRefQuery(queryEvent).join();

        final RefRefreshEvent refreshEvent = RefRefreshEvent.of(queryEvent.getRef(), TEAM_MEMBER, member.getId());

        final ArgumentCaptor<Event> refreshCaptor = ArgumentCaptor.forClass(Event.class);
        verify(emitter, times(5)).emit(refreshCaptor.capture());
        assertTrue(refreshCaptor.getAllValues().contains(refreshEvent));
    }

    @Test
    void refRefresh() throws Exception {

        final Map<String, Instance> init = database.batch(Caller.SUPER, BatchOptions.builder()
                .putAction("team", CreateOptions.builder()
                        .setSchema(TEAM)
                        .setId("t1")
                        .build())
                .putAction("member", CreateOptions.builder()
                        .setSchema(TEAM_MEMBER)
                        .setData(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "u1"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();
        assertEquals(2, init.size());

        final Instance member = init.get("member");
        assertNotNull(member);

        assertNotNull(database.read(Caller.SUPER, TEAM, "t1").join());

        final Map<String, Instance> update = database.batch(Caller.SUPER, BatchOptions.builder()
                .putAction("team", UpdateOptions.builder()
                        .setSchema(TEAM)
                        .setId("t1")
                        .setData(ImmutableMap.of(
                                "name", "Test"
                        ))
                        .build())
                .build()).get();
        assertEquals(1, update.size());

        final RefRefreshEvent refreshEvent = RefRefreshEvent.of(Ref.of(TEAM, "t1"), TEAM_MEMBER, member.getId());

        database.onRefRefresh(refreshEvent).get();

        final Page<Instance> get = database.query(Caller.SUPER, TEAM_MEMBER, Expression.parse("team.name == 'Test'")).get();
        assertEquals(1, get.size());
    }

    @Test
    void deepExpandRefresh() throws Exception {

        final Instance teamA = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(TEAM)
                .setData(ImmutableMap.of(
                        "name", "a"
                ))
                .build()).get();

        final Instance teamB = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(TEAM)
                .setData(ImmutableMap.of(
                        "name", "b",
                        "parent", ReferableSchema.ref(teamA.getId())
                ))
                .build()).get();

        final Instance member = database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(TEAM_MEMBER)
                .setData(ImmutableMap.of(
                        "team", ReferableSchema.ref(teamB.getId())
                ))
                .build()).get();

        final Instance updateTeamA = database.update(Caller.SUPER, UpdateOptions.builder()
                .setSchema(TEAM)
                .setId(teamA.getId())
                .setData(ImmutableMap.of(
                        "name", "a2"
                ))
                .build()).get();

        final ObjectUpdatedEvent updated = ObjectUpdatedEvent.of(TEAM, teamA.getId(), 1L, teamA, updateTeamA);
        database.onObjectUpdated(updated).get();

        database.onRefQuery(RefQueryEvent.of(Ref.of(TEAM, teamA.getId()), TEAM_MEMBER, new Or(
                new Eq(new NameConstant(Name.parse("team.id")), new Constant(teamA.getId())),
                new Eq(new NameConstant(Name.parse("team.parent.id")), new Constant(teamA.getId()))))).get();
        database.onRefRefresh(RefRefreshEvent.of(Ref.of(TEAM, teamA.getId()), TEAM_MEMBER, member.getId()));

        final Instance get = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(TEAM_MEMBER)
                .setId(member.getId())
                .setExpand(Name.parseSet("team.parent"))
                .build()).get();

        assertNotNull(get);
        assertEquals("a2", Instance.get(get, Name.parse("team.parent.name")));
    }

//    @Test
//    void aggregate() throws Exception {
//
//        database.batch(Caller.SUPER, BatchOptions.builder()
//                .action("a", CreateOptions.builder()
//                        .setSchema(TEAM_MEMBER)
//                        .setData(ImmutableMap.of(
//                                "user", ImmutableMap.of("id", "u1"),
//                                "team", ImmutableMap.of("id", "t1"),
//                                "role", "owner",
//                                "accepted", true
//                        )).build())
//                .build()).get();
//
//        final Page<Instance> results = database.query(Caller.SUPER, QueryOptions.builder()
//                .setSchema(Name.of("TeamMemberStats"))
//                .build()).get();
//
//        log.debug("Aggregate results: {}", results);
//    }

    @Test
    void merge() throws Exception {

        final String id = UUID.randomUUID().toString();

        database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(SIMPLE)
                .setId(id)
                .setData(ImmutableMap.of(
                        "boolean", true,
                        "number", 5,
                        "string", "hello",
                        "map", ImmutableMap.of(
                                "hello", "world"
                        )
                ))
                .build()).get();

        final Instance merged = database.update(Caller.SUPER, UpdateOptions.builder()
                .setSchema(SIMPLE)
                .setId(id)
                .setMode(UpdateOptions.Mode.MERGE)
                .setData(ImmutableMap.of(
                        "map", ImmutableMap.of(
                                "goodbye", "blue sky"
                        )
                ))
                .build()).get();

        assertObject(SIMPLE, id, 2, ImmutableMap.of(
                "boolean", true,
                "number", 5,
                "string", "hello",
                "map", ImmutableMap.of(
                        "goodbye", "blue sky"
                )
        ), merged);

        final Instance deepMerged = database.update(Caller.SUPER, UpdateOptions.builder()
                .setSchema(SIMPLE)
                .setId(id)
                .setMode(UpdateOptions.Mode.MERGE_DEEP)
                .setData(ImmutableMap.of(
                        "map", ImmutableMap.of(
                                "hello", "world"
                        )
                ))
                .build()).get();

        assertObject(SIMPLE, id, 3, ImmutableMap.of(
                "boolean", true,
                "number", 5,
                "string", "hello",
                "map", ImmutableMap.of(
                        "hello", "world",
                        "goodbye", "blue sky"
                )
        ), deepMerged);
    }

    @Test
    void testInvalidRefError() {

        assertThrows(ConstraintViolationException.class, cause(() -> {
            database.create(Caller.SUPER, CreateOptions.builder()
                    .setSchema(REF_SOURCE)
                    .setData(ImmutableMap.of(
                            "target", "x"
                    ))
                    .build()).get();
        }));
    }

    @Test
    void testInvalidEnumError() {

        assertThrows(ConstraintViolationException.class, cause(() -> {
            database.create(Caller.SUPER, CreateOptions.builder()
                    .setSchema(WITH_ENUM)
                    .setData(ImmutableMap.of(
                            "value", "C"
                    ))
                    .build()).get();
        }));
    }

    @Test
    void testDatabaseReadonly() {

        final Database database = DatabaseServer.builder().namespace(namespace).storage(storage)
                .emitter(emitter).mode(DatabaseMode.READONLY).build();
        assertThrows(DatabaseReadonlyException.class, () -> database.create(Caller.SUPER, CreateOptions.builder()
                    .setSchema(SIMPLE)
                    .setData(ImmutableMap.of())
                    .build()).get());
    }

    @Test
    void testSchemaReadonly() {

        assertThrows(UnsupportedWriteException.class, () -> database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(READONLY)
                .setData(ImmutableMap.of())
                .build()).get());
    }

    @Test
    void testVersionedRef() throws Exception {

        final String id = UUID.randomUUID().toString();

        database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(SIMPLE)
                .setId(id)
                .setData(ImmutableMap.of(
                        "boolean", true
                ))
                .build()).get();

        database.update(Caller.SUPER, UpdateOptions.builder()
                .setSchema(SIMPLE)
                .setId(id)
                .setData(ImmutableMap.of(
                        "boolean", false
                ))
                .build()).get();

        database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(WITH_VERSIONED_REF)
                .setId(id)
                .setData(ImmutableMap.of(
                        "ref", ReferableSchema.versionedRef(id, 1L)
                ))
                .build()).get();

        final Instance read = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(WITH_VERSIONED_REF)
                .setId(id)
                .setExpand(Name.parseSet("ref"))
                .build()).get();

        assertEquals(1L, Instance.<Long>get(read, Name.parse("ref.version")));
        assertEquals(true,  Instance.get(read, Name.parse("ref.boolean")));
    }

    @Test
    void testSecret() throws Exception {

        final String id = UUID.randomUUID().toString();

        database.create(Caller.SUPER, CreateOptions.builder()
                .setSchema(USER)
                .setId(id)
                .setData(ImmutableMap.of(
                        "password", Secret.encrypted("ABC")
                ))
                .build()).get();

        final Instance read = database.read(Caller.SUPER, ReadOptions.builder()
                .setSchema(USER)
                .setId(id)
                .build()).get();

        assertEquals(
                Secret.encrypted("ABC"),
                Instance.<Secret>get(read, Name.parse("password"))
        );
    }

    @Test
    void testRefQueryEvents() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Set<Event> events = database.refQueryEvents(namespace.requireObjectSchema("NestedRef3"), id);
        System.err.println(events);
        final Set<Name> expand = namespace.requireObjectSchema("NestedRef1").refExpand(
                namespace.requireObjectSchema("NestedRef2").getQualifiedName(),
                namespace.requireObjectSchema("NestedRef1").getExpand()
        );
        System.err.println(expand);
    }

    private Executable cause(final Executable target) {

        return () -> {
            try {
                target.execute();
            } catch (final Throwable e) {
                assertNotNull(e.getCause());
                throw e.getCause();
            }
        };
    }

    @Test
    void testPassStats() throws Exception {

        final Set<Page.Stat> expected = ImmutableSet.of(Page.Stat.TOTAL);

        final Storage storage = mock(Storage.class);
        when(storage.query(any(), any(), any(), any(), any()))
                .thenReturn(((stats, token, count) -> {
                    assertEquals(expected, stats);
                    return CompletableFuture.completedFuture(Page.empty());
                }));
        final Database database = DatabaseServer.builder()
                .namespace(namespace).storage(storage)
                .emitter(emitter).build();
        final Caller caller = Mockito.mock(Caller.class);

        database.query(caller, QueryOptions.builder()
                .setSchema(SIMPLE)
                .setStats(expected)
                .build()).get();
    }
}
