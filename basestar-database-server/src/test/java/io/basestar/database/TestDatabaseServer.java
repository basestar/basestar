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
import io.basestar.database.options.*;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.Ref;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.util.PagedList;
import io.basestar.util.Path;
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
public class TestDatabaseServer {

    private static final String SIMPLE = "Simple";

    private static final String INDEXED = "Indexed";

    private static final String MULTI_INDEXED = "MultiIndexed";

    private static final String REF_SOURCE = "RefSource";

    private static final String REF_TARGET = "RefTarget";

    private static final String SIMPLE_PERMS = "SimplePerms";

    private static final String CUSTOM_ID = "CustomId";

    private static final String ANIMAL = "Animal";

    private static final String TEAM = "Team";

    private static final String TEAM_MEMBER = "TeamMember";

    private static final String CAT = "Cat";

    private static final String DOG = "Dog";

    private static final String KENNEL = "Kennel";

    private static final String USER = "User";

    private static final String VISIBILITY = "Visibility";

    private static final String TRANSIENT = "Transient";

    private DatabaseServer database;

    private Storage storage;

    private Emitter emitter;

    private Caller caller;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {

        final Namespace namespace = Namespace.load(
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
        this.database = new DatabaseServer(namespace, storage, emitter);
        this.caller = Mockito.mock(Caller.class);
    }

    private static void assertObject(final String schema, final String id, final long version, final Map<String, Object> data, final Map<String, Object> object) {

        assertEquals(schema, Instance.getSchema(object));
        assertEquals(id, Instance.getId(object));
        assertEquals((Long)version, Instance.getVersion(object));
        assertNotNull(Instance.getHash(object));
        assertNotNull(Instance.getCreated(object));
        assertNotNull(Instance.getUpdated(object));
        data.forEach((k, v) -> assertEquals(v, data.get(k)));
    }

    @Test
    public void createSimple() throws Exception {

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
    public void updateSimple() throws Exception {

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
    public void deleteSimple() throws Exception {

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
    public void createIndexed() throws Exception {

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

        final PagedList<Instance> queryA = database.query(caller, INDEXED, Expression.parse("value == 'a'")).get();
        assertEquals(1, queryA.size());
        assertEquals(readA, queryA.get(0));

        final PagedList<Instance> queryB = database.query(caller, INDEXED, Expression.parse("value == 'b'")).get();
        assertEquals(1, queryB.size());
        assertEquals(readB, queryB.get(0));
    }

    @Test
    public void createMultiIndexed() throws Exception {

        final String idA = UUID.randomUUID().toString();
        final Map<String, Object> dataA = ImmutableMap.of(
                "value", ImmutableList.of(
                        "a", "b", "c"
                )
        );

        final Map<String, Object> createA = database.create(caller, MULTI_INDEXED, idA, dataA).get();
        assertObject(MULTI_INDEXED, idA, 1, dataA, createA);

//        final Map<String, Object> readA = database.read(caller, INDEXED, idA).get();
//        assertEquals(createA, readA);
//
//        final String idB = UUID.randomUUID().toString();
//        final Map<String, Object> dataB = ImmutableMap.of(
//                "value", "b"
//        );
//
//        final Map<String, Object> createB = database.create(caller, INDEXED, idB, dataB).get();
//        assertObject(INDEXED, idB, 1, dataB, createB);
//
//        final Map<String, Object> readB = database.read(caller, INDEXED, idB).get();
//        assertEquals(createB, readB);
//
//        final PagedList<Instance> queryA = database.query(caller, INDEXED, Expression.parse("value == 'a'")).get();
//        assertEquals(1, queryA.size());
//        assertEquals(readA, queryA.get(0));
//
//        final PagedList<Instance> queryB = database.query(caller, INDEXED, Expression.parse("value == 'b'")).get();
//        assertEquals(1, queryB.size());
//        assertEquals(readB, queryB.get(0));
    }

    @Test
    public void createRef() throws Exception {

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

        final Map<String, Object> readA = database.read(caller, ReadOptions.builder().schema(REF_SOURCE)
                .id(idA).expand(Path.parseSet("target")).build()).get();
        assertEquals(createRefA, readA.get("target"));

        final PagedList<Instance> linkA = database.queryLink(caller, REF_TARGET, refA, "sources").get();
        assertEquals(1, linkA.size());
        assertEquals(createA, linkA.get(0));

        final Map<String, Object> expandLinkA = database.read(caller, ReadOptions.builder().schema(REF_TARGET)
                .id(refA).expand(Path.parseSet("sources")).build()).get();
        final PagedList<?> source = (PagedList<?>)expandLinkA.get("sources");
        assertEquals(1, source.size());
        assertEquals(createA, source.get(0));

        final PagedList<Instance> expandQuery = database.query(caller, QueryOptions.builder()
                .schema(REF_SOURCE)
                .expression(Expression.parse("target.id == \"" + refA + "\""))
                .expand(ImmutableSet.of(Path.of("target")))
                .build()).get();
        assertEquals(1, expandQuery.size());
        final Instance queryTarget = expandQuery.get(0).get("target", Instance.class);
        assertNotNull(queryTarget);
        assertNotNull(queryTarget.getHash());
    }

    @Test
    public void nestedRef() throws Exception {

        final String idA = "a";
        final Map<String, Object> createRefA = database.create(caller, REF_TARGET, idA, ImmutableMap.of(
                "value", "a"
        )).get();

        final String idB = "b";
        final Map<String, Object> createRefB = database.create(caller, CreateOptions.builder().schema(REF_TARGET)
                .id(idB).data(ImmutableMap.of(
                "value", "b",
                "target", ImmutableMap.of(
                        "id", idA
                )
        )).expand(Path.parseSet("target")).build()).get();
        // Check reading refs doesn't wipe properties
        assertNotNull(createRefB.get("value"));
        assertEquals(createRefA, createRefB.get("target"));

        //System.err.println(Path.parseSet("target.target"));

        final String idC = UUID.randomUUID().toString();
        final Map<String, Object> createRefC = database.create(caller, CreateOptions.builder().schema(REF_TARGET)
                .id(idC).data(ImmutableMap.of(
                "value", "c",
                "target", ImmutableMap.of(
                        "id", idB
                )
        )).expand(Path.parseSet("target.target")).build()).get();
        assertEquals(createRefB, createRefC.get("target"));
    }

    @Test
    public void missingRefNotNull() throws Exception {

        final String missing = UUID.randomUUID().toString();
        final String refA = UUID.randomUUID().toString();
        final Map<String, Object> createRefA = database.create(caller, CreateOptions.builder().schema(REF_SOURCE)
                .id(refA).data(ImmutableMap.of(
                "value", "test",
                "target", ImmutableMap.of(
                        "id", missing
                )
        )).expand(Path.parseSet("target")).build()).get();
        @SuppressWarnings("unchecked")
        final Map<String, Object> target = (Map<String, Object>)createRefA.get("target");
        assertNotNull(target);
//        assertEquals(REF_TARGET, Instance.getSchema(target));
        assertEquals(missing, Instance.getId(target));
    }

    @Test
    public void simplePerms() throws Exception {

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
    public void customId() throws Exception {

        final Map<String, Object> createA = database.create(Caller.SUPER, CUSTOM_ID, ImmutableMap.of(
                "x", "x"
        )).get();
        assertNotNull(createA);
        assertEquals("custom:x", Instance.getId(createA));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void polymorphicCreate() {

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

        final PagedList<Instance> queryA = database.query(caller, ANIMAL, Expression.parse("name == 'Pippa'")).join();
        assertEquals(1, queryA.size());
        assertEquals(CAT, queryA.get(0).getSchema());
        assertEquals("Bengal", queryA.get(0).get("breed"));

        final PagedList<Instance> queryB = database.query(caller, ANIMAL, Expression.parse("class == 'Mammal'")).join();
        assertEquals(2, queryB.size());

        final PagedList<Instance> queryC = database.query(caller, CAT, Expression.parse("class == 'Mammal'")).join();
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

        final Set<Path> expand = ImmutableSet.of(Path.of("residents"));
        final Instance readC = database.read(caller, ReadOptions.builder().schema(KENNEL)
                .id(idC).expand(expand).build()).join();
        final Collection<Map<String, Object>> residents = (Collection<Map<String, Object>>)readC.get("residents");
        assertTrue(residents.stream().allMatch(v -> v.get("breed") != null));
    }

    @Test
    public void batch() {

        final Map<String, Instance> results = database.transaction(caller, TransactionOptions.builder()
                .action("a", CreateOptions.builder()
                        .schema(SIMPLE)
                        .expressions(ImmutableMap.of(
                                "string", Expression.parse("batch.c.id")
                        ))
                        .build())
                .action("b", CreateOptions.builder()
                        .schema(SIMPLE)
                        .data(ImmutableMap.of(
                                "string", "b"
                        ))
                        .expressions(ImmutableMap.of(
                                "array", Expression.parse("[batch.a.id]")
                        ))
                        .build())
                .action("c", CreateOptions.builder()
                        .schema(SIMPLE)
                        .data(ImmutableMap.of(
                                "string", "a"
                        ))
                        .build())
                .build()).join();

        System.err.println(results);
    }

    @Test
    public void advancedPerms() throws Exception {

        when(caller.getSchema()).thenReturn(USER);
        when(caller.getId()).thenReturn("test");

        final Map<String, Instance> ok = database.transaction(caller, TransactionOptions.builder()
                .action("team", CreateOptions.builder()
                        .schema(TEAM)
                        .id("t1")
                        .build())
                .action("member", CreateOptions.builder()
                        .schema(TEAM_MEMBER)
                        .data(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "test"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();
        assertEquals(2, ok.size());

        assertThrows(PermissionDeniedException.class, cause(() ->
                database.transaction(caller, TransactionOptions.builder()
                        .action("team", CreateOptions.builder()
                                .schema(TEAM)
                                .id("t2")
                                .build())
                        .action("member", CreateOptions.builder()
                                .schema(TEAM_MEMBER)
                                .data(ImmutableMap.of(
                                        "user", ImmutableMap.of("id", "test"),
                                        "team", ImmutableMap.of("id", "t2"),
                                        "role", "owner",
                                        "accepted", false
                                )).build())
                        .build()).get()));
    }

    @Test
    public void visibility() throws Exception {

        final Instance createA = database.create(Caller.SUPER, CreateOptions.builder()
                .schema(VISIBILITY)
                .data(ImmutableMap.of(
                        "x", 2
                ))
                .build()).get();
        assertNull(createA.get("x"));

        final Instance readA = database.read(Caller.SUPER, ReadOptions.builder()
                .schema(VISIBILITY)
                .id(createA.getId())
                .build()).get();
        assertNull(readA.get("x"));

        final Instance createB = database.create(Caller.SUPER, CreateOptions.builder()
                .schema(VISIBILITY)
                .data(ImmutableMap.of(
                        "x", 20
                ))
                .build()).get();
        assertNotNull(createB.get("x"));

        final Instance readB = database.read(Caller.SUPER, ReadOptions.builder()
                .schema(VISIBILITY)
                .id(createB.getId())
                .build()).get();
        assertNotNull(readB.get("x"));
    }

    @Test
    public void transients() throws Exception {

        final Instance createA = database.create(Caller.SUPER, CreateOptions.builder()
                .schema(TRANSIENT)
                .data(ImmutableMap.of(
                        "name", "test"
                ))
                .build()).get();

        final Instance createB = database.create(Caller.SUPER, CreateOptions.builder()
                .schema(TRANSIENT)
                .data(ImmutableMap.of(
                        "refs", ImmutableList.of(createA)
                ))
                .expand(ImmutableSet.of(Path.of("names")))
                .build()).get();
        assertEquals(ImmutableList.of("test"), createB.get("names"));

        final Instance readB = database.read(Caller.SUPER, ReadOptions.builder()
                .schema(TRANSIENT)
                .id(createB.getId())
                .expand(ImmutableSet.of(Path.of("names")))
                .build()).get();
        assertEquals(ImmutableList.of("test"), readB.get("names"));
    }

    @Test
    public void expand() throws Exception {

        final Map<String, Instance> ok = database.transaction(Caller.SUPER, TransactionOptions.builder()
                .action("team", CreateOptions.builder()
                        .schema(TEAM)
                        .id("t1")
                        .build())
                .action("user", CreateOptions.builder()
                        .schema(USER)
                        .id("u1")
                        .build())
                .action("member", CreateOptions.builder()
                        .schema(TEAM_MEMBER)
                        .data(ImmutableMap.of(
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

        final RefQueryEvent queryEvent = RefQueryEvent.of(Ref.of(TEAM, "t1"), TEAM_MEMBER, Expression.parse("team.id == 't1'").bind(Context.init()));

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
    public void refRefresh() throws Exception {

        final Map<String, Instance> init = database.transaction(Caller.SUPER, TransactionOptions.builder()
                .action("team", CreateOptions.builder()
                        .schema(TEAM)
                        .id("t1")
                        .build())
                .action("member", CreateOptions.builder()
                        .schema(TEAM_MEMBER)
                        .data(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "u1"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();
        assertEquals(2, init.size());

        final Instance member = init.get("member");
        assertNotNull(member);

        final Map<String, Instance> update = database.transaction(Caller.SUPER, TransactionOptions.builder()
                .action("team", UpdateOptions.builder()
                        .schema(TEAM)
                        .id("t1")
                        .data(ImmutableMap.of(
                                "name", "Test"
                        ))
                        .build())
                .build()).get();
        assertEquals(2, init.size());

        final RefRefreshEvent refreshEvent = RefRefreshEvent.of(Ref.of(TEAM, "t1"), TEAM_MEMBER, member.getId());

        database.onRefRefresh(refreshEvent).join();

        final PagedList<Instance> get = database.query(Caller.SUPER, TEAM_MEMBER, Expression.parse("team.name == 'Test'")).join();
        assertEquals(1, get.size());
    }

    @Test
    public void aggregate() throws Exception {

        database.transaction(Caller.SUPER, TransactionOptions.builder()
                .action("a", CreateOptions.builder()
                        .schema(TEAM_MEMBER)
                        .data(ImmutableMap.of(
                                "user", ImmutableMap.of("id", "u1"),
                                "team", ImmutableMap.of("id", "t1"),
                                "role", "owner",
                                "accepted", true
                        )).build())
                .build()).get();

        final PagedList<Instance> results = database.query(Caller.SUPER, QueryOptions.builder()
                .schema("TeamMemberStats")
                .build()).get();

        System.err.println(results);
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
}
