package io.basestar.database;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.PermissionDeniedException;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.database.options.CreateOptions;
import io.basestar.database.options.DeleteOptions;
import io.basestar.database.options.ReadOptions;
import io.basestar.database.options.UpdateOptions;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.util.PagedList;
import io.basestar.util.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

//import io.basestar.database.storage.LoggingStorage;

public class TestDatabaseServer {

    private static final String SIMPLE = "Simple";

    private static final String INDEXED = "Indexed";

    private static final String MULTI_INDEXED = "MultiIndexed";

    private static final String REF_SOURCE = "RefSource";

    private static final String REF_TARGET = "RefTarget";

    private static final String SIMPLE_PERMS = "SimplePerms";

    private static final String ANIMAL = "Animal";

    private static final String CAT = "Cat";

    private static final String DOG = "Dog";

    private static final String KENNEL = "Kennel";

    private static final String USER = "User";

    private Database database;

    private Storage storage;

    private Emitter emitter;

    private Caller caller;

    @BeforeEach
    public void setUp() throws Exception {

        final Namespace namespace = Namespace.load(TestDatabaseServer.class.getResource("/io/basestar/database/schema.json"));
        this.emitter = Mockito.mock(Emitter.class);
        when(emitter.emit(any(Event.class))).thenReturn(CompletableFuture.completedFuture(null));
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

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, new ReadOptions().setVersion(1L)).get();
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

        final Map<String, Object> update = database.update(caller, SIMPLE, id, data2, new UpdateOptions().setVersion(1L)).get();
        assertObject(SIMPLE, id, 2, data2, update);

        final Map<String, Object> read = database.read(caller, SIMPLE, id).get();
        assertEquals(update, read);

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, new ReadOptions().setVersion(1L)).get();
        assertEquals(create, version1);

        final Map<String, Object> version2 = database.read(caller, SIMPLE, id, new ReadOptions().setVersion(2L)).get();
        assertEquals(read, version2);

        verify(emitter, times(1))
                .emit(ObjectUpdatedEvent.of(SIMPLE, id, 1L, create, update));
    }

    @Test
    public void deleteSimple() throws Exception {

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of();
        final Map<String, Object> create = database.create(caller, SIMPLE, id, data).get();

        final boolean delete = database.delete(caller, SIMPLE, id, new DeleteOptions().setVersion(1L)).get();
        assertTrue(delete);

        final Map<String, Object> read = database.read(caller, SIMPLE, id).get();
        assertNull(read);

        final Map<String, Object> version1 = database.read(caller, SIMPLE, id, new ReadOptions().setVersion(1L)).get();
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

        final Map<String, Object> readA = database.read(caller, REF_SOURCE, idA, new ReadOptions().setExpand(Path.parseSet("target"))).get();
        assertEquals(createRefA, readA.get("target"));

        final PagedList<Instance> linkA = database.page(caller, REF_TARGET, refA, "sources").get();
        assertEquals(1, linkA.size());
        assertEquals(createA, linkA.get(0));

        final Map<String, Object> expandLinkA = database.read(caller, REF_TARGET, refA, new ReadOptions().setExpand(Path.parseSet("sources"))).get();
        final PagedList<?> source = (PagedList<?>)expandLinkA.get("sources");
        assertEquals(1, source.size());
        assertEquals(createA, source.get(0));
    }

    @Test
    public void nestedRef() throws Exception {

        final String idA = "a";
        final Map<String, Object> createRefA = database.create(caller, REF_TARGET, idA, ImmutableMap.of(
                "value", "a"
        )).get();

        final String idB = "b";
        final Map<String, Object> createRefB = database.create(caller, REF_TARGET, idB, ImmutableMap.of(
                "value", "b",
                "target", ImmutableMap.of(
                        "id", idA
                )
        ), new CreateOptions().setExpand(Path.parseSet("target"))).get();
        // Check reading refs doesn't wipe properties
        assertNotNull(createRefB.get("value"));
        assertEquals(createRefA, createRefB.get("target"));

        //System.err.println(Path.parseSet("target.target"));

        final String idC = UUID.randomUUID().toString();
        final Map<String, Object> createRefC = database.create(caller,  REF_TARGET, idC, ImmutableMap.of(
                "value", "c",
                "target", ImmutableMap.of(
                        "id", idB
                )
        ), new CreateOptions().setExpand(Path.parseSet("target.target"))).get();
        assertEquals(createRefB, createRefC.get("target"));
    }

    @Test
    public void missingRefNotNull() throws Exception {

        final String missing = UUID.randomUUID().toString();
        final String refA = UUID.randomUUID().toString();
        final Map<String, Object> createRefA = database.create(caller, REF_SOURCE, refA, ImmutableMap.of(
                "value", "test",
                "target", ImmutableMap.of(
                        "id", missing
                )
        ), new CreateOptions().setExpand(Path.parseSet("target"))).get();
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
        final Instance readC = database.read(caller, KENNEL, idC, new ReadOptions().setExpand(expand)).join();
        final Collection<Map<String, Object>> residents = (Collection<Map<String, Object>>)readC.get("residents");
        assertTrue(residents.stream().allMatch(v -> v.get("breed") != null));
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
