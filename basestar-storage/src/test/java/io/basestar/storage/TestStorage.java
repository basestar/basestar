package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.type.Values;
import io.basestar.schema.*;
import io.basestar.schema.encoding.FlatEncoding;
import io.basestar.secret.Secret;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.test.CsvUtils;
import io.basestar.util.Streams;
import io.basestar.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Slf4j
@SuppressWarnings(Warnings.JUNIT_VISIBILITY)
public abstract class TestStorage {

    protected static final int RECORD_COUNT = 100;

    protected static final String ADDRESS = "Address";

    protected static final String ADDRESS_STATS = "AddressStats";

    protected static final String SIMPLE = "Simple";

    protected static final String POINTSET = "Pointset";

    protected static final String REF_TARGET = "RefTarget";

    protected static final String REF_SOURCE = "RefSource";

    protected static final String DATE_SORT = "DateSort";

    protected static final String EXPANDED = "Expanded";

    protected static final String VERSIONED_REF = "VersionedRef";

    protected static final String INTERFACE = "Interface";

    protected static final String EXTEND_A = "ExtendA";

    protected static final String EXTEND_B = "ExtendB";

    protected static final String SECRET = "Secret";

    protected final Namespace namespace;

    protected TestStorage() {

        try {
            this.namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract Storage storage(Namespace namespace);

    protected boolean supportsDelete() {

        return true;
    }

    protected boolean supportsUpdate() {

        return true;
    }

    protected boolean supportsIndexes() {

        return true;
    }

    protected boolean supportsOversize() {

        return true;
    }

    protected void bulkLoad(final Storage storage, final Multimap<String, Map<String, Object>> data) {

        writeAll(storage, namespace, data);
    }

    // FIXME: merge with createComplete

    protected void writeAll(final Storage storage, final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        if(!data.isEmpty()) {
            final Storage.WriteTransaction write = storage.write(Consistency.QUORUM, Versioning.CHECKED);

            data.asMap().forEach((k, vs) -> {
                final ObjectSchema schema = namespace.requireObjectSchema(k);
                vs.forEach(v -> {
                    final Instance instance = schema.create(v);
                    final String id = Instance.getId(v);
                    write.createObject(schema, id, instance);
                });
            });

            write.write().join();
        }
    }

    // FIXME: merge with createComplete

    protected Multimap<String, Map<String, Object>> loadAddresses() throws IOException {

        final Instant now = ISO8601.now();
        final Multimap<String, Map<String, Object>> results = ArrayListMultimap.create();

        try(final InputStream is = TestStorage.class.getResourceAsStream("/data/Petstore/Address.csv")) {
            final CSVParser parser = CSVParser.parse(is, Charsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            final List<String> headers = parser.getHeaderNames();
            Streams.stream(parser).forEach(record -> {

                final Map<String, Object> data = new HashMap<>();
                headers.forEach(h -> data.put(h, record.get(h)));
                Instance.setVersion(data, 1L);
                Instance.setCreated(data, now);
                Instance.setUpdated(data, now);

                results.put(ADDRESS, data);
            });
        }

        return results;
    }

    @Test
    protected void testIndexes() throws IOException {

        assumeTrue(supportsIndexes());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(ADDRESS);

        bulkLoad(storage, loadAddresses());

        final List<Sort> sort = ImmutableList.of(
                Sort.asc(Name.of("city")),
                Sort.asc(Name.of("zip"))
        );

        final Expression expr = Expression.parse("country == 'US' || state == 'England'");
        final Page<Map<String, Object>> results = storage.query(Consistency.ATOMIC, schema, expr, sort, Collections.emptySet()).page(EnumSet.of(Page.Stat.TOTAL), null, 10).join();

        final List<String> ids = Arrays.asList("123", "189", "26", "67", "129", "159", "151", "116", "142", "179");
        assertEquals(ids, results.map(Instance::getId).getPage());
        if(results.getStats() != null && results.getStats().getTotal() != null) {
            assertEquals(16, results.getStats().getTotal());
        }
    }

    @Test
    protected void testEmptyStorageQuery() {

        // Storage should not throw an error when it has not received writes (relevant to on-demand provisioned storage)

        assumeTrue(supportsIndexes());

        final Storage storage = storage(namespace);
        final ObjectSchema schema = namespace.requireObjectSchema(ADDRESS);

        final List<Sort> sort = ImmutableList.of(
                Sort.asc(Name.of("city")),
                Sort.asc(Name.of("zip"))
        );

        final Expression expr = Expression.parse("country == 'US' || state == 'England'");
        final Page<Map<String, Object>> results = storage.query(Consistency.ATOMIC, schema, expr, sort, Collections.emptySet()).page(EnumSet.of(Page.Stat.TOTAL), null, 10).join();

        assertEquals(ImmutableList.of(), results.getPage());
    }

    // FIXME: needs to cover non-trivial case(s)

    @Test
    protected void testSortAndPaging() {

        assumeTrue(supportsIndexes());

        final Instant now = ISO8601.now();

        // Horrible index usage, but high storage support
        final String country = UUID.randomUUID().toString();
        final Multimap<String, Map<String, Object>> init = HashMultimap.create();
        for(int i = 0; i != 100; ++i) {

            final String id = UUID.randomUUID().toString();

            final Map<String, Object> data = new HashMap<>();
            data.put("country", country);
            data.put("city", UUID.randomUUID().toString());
            data.put("zip", UUID.randomUUID().toString());
            Instance.setId(data, id);
            Instance.setVersion(data, 1L);
            Instance.setCreated(data, now);
            Instance.setUpdated(data, now);

            init.put(ADDRESS, data);
        }

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(ADDRESS);

        bulkLoad(storage, init);

        final List<Sort> sort = ImmutableList.of(
                Sort.asc(Name.of("city")),
                Sort.asc(Name.of("zip"))
        );

        final Expression expr = Expression.parse("country == '" + country + "'");
        final Pager<Map<String, Object>> pager = storage.query(Consistency.EVENTUAL, schema, expr, sort, Collections.emptySet());

        final Set<String> results = new HashSet<>();
        Page.Token token = null;
        for(int i = 0; i != 10; ++i) {
            final Page<Map<String, Object>> page = pager.page(ImmutableSet.of(Page.Stat.TOTAL, Page.Stat.APPROX_TOTAL), token,10).join();
            final Page.Stats stats = page.getStats();
            if(stats != null) {
                if(stats.getTotal() != null) {
                    assertEquals(100, stats.getTotal());
                }
                if(stats.getApproxTotal() != null) {
                    assertEquals(100, stats.getApproxTotal());
                }
            }
            page.forEach(object -> results.add(Instance.getId(object)));
            token = page.getPaging();
        }
        assertEquals(100, results.size());
    }

    @Test
    protected void testCreate() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = data();

        final Instance after = instance(schema, id, 1L, data);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, after)
                .write().join();

        final Map<String, Object> current = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());
        assertNotNull(current);
        assertEquals(1, Instance.getVersion(current));
        data.forEach((k, v) -> {
            final Object v2 = current.get(k);
            assertTrue(Values.equals(v, v2), k + ": " + v + " != " + v2);
        });

        if(storage.storageTraits(schema).getHistoryConsistency().isStronger(Consistency.EVENTUAL)) {
            final Map<String, Object> v1 = storage.getVersion(Consistency.ATOMIC, schema, id, 1L, ImmutableSet.of()).join();
            assertNotNull(v1);
            assertEquals(1, Instance.getVersion(v1));
        }
    }

    @Test
    protected void testEmptyString() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of("string", "");

        final Instance after = instance(schema, id, 1L, data);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, after)
                .write().join();

        final Map<String, Object> current = storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join();
        assertNotNull(current);
        assertEquals("", current.get("string"));
    }

    private Map<String, Object> data() {

        return ImmutableMap.<String, Object>builder()
                .put("boolean", true)
                .put("integer", 1L)
                .put("number", 2.5)
                .put("decimal", BigDecimal.valueOf(3.5))
                .put("string", "test")
                .put("binary", Bytes.valueOf(1, 2, 3, 4))
                .put("date", ISO8601.parseDate("2030-01-01"))
                .put("datetime", ISO8601.parseDateTime("2030-01-01T01:02:03Z"))
                .put("struct", new Instance(ImmutableMap.of("x", 1L, "y", 5L)))
                .put("object", new Instance(ImmutableMap.of("id", "test")))
                .put("arrayBoolean", Collections.singletonList(true))
                .put("arrayInteger", Collections.singletonList(1L))
                .put("arrayNumber", Collections.singletonList(2.5))
                .put("arrayDecimal", Collections.singletonList(BigDecimal.valueOf(3.5)))
                .put("arrayString", Collections.singletonList("test"))
                .put("arrayBinary", Collections.singletonList(Bytes.valueOf(1, 2, 3, 4)))
                .put("arrayStruct", Collections.singletonList(new Instance(ImmutableMap.of("x", 10L, "y", 5L))))
                .put("arrayObject", Collections.singletonList(new Instance(ImmutableMap.of("id", "test"))))
                .put("arrayDate", Collections.singletonList(ISO8601.parseDate("2030-01-01")))
                .put("arrayDatetime", Collections.singletonList(ISO8601.parseDateTime("2030-01-01T01:02:03Z")))
                .put("mapBoolean", Collections.singletonMap("a", true))
                .put("mapInteger", Collections.singletonMap("a", 1L))
                .put("mapNumber", Collections.singletonMap("a", 2.5))
                .put("mapDecimal", Collections.singletonMap("a", BigDecimal.valueOf(3.5)))
                .put("mapString", Collections.singletonMap("a", "test"))
                .put("mapBinary", Collections.singletonMap("a", Bytes.valueOf(1, 2, 3, 4)))
                .put("mapStruct", Collections.singletonMap("a",new Instance(ImmutableMap.of("x", 10L, "y", 5L))))
                .put("mapObject", Collections.singletonMap("a", new Instance(ImmutableMap.of("id", "test"))))
                .put("mapDate", Collections.singletonMap("a", ISO8601.parseDate("2030-01-01")))
                .put("mapDatetime", Collections.singletonMap("a", ISO8601.parseDateTime("2030-01-01T01:02:03Z")))
                .build();
    }

    @Test
    protected void testUpdate() {

        assumeTrue(supportsUpdate());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());
        assertEquals(1L, before.getVersion());

        final Instance after = instance(schema, id, 2L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, setVersion(before, 1L), after)
                .write().join();

        final Map<String, Object> current = storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join();
        assertNotNull(current);
        assertEquals(2, Instance.getVersion(current));
        if(storage.storageTraits(schema).getHistoryConsistency().isStronger(Consistency.EVENTUAL)) {
            final Map<String, Object> v2 = storage.getVersion(Consistency.ATOMIC, schema, id, 2L, ImmutableSet.of()).join();
            assertNotNull(v2);
            assertEquals(2, Instance.getVersion(v2));
        }
    }

    private Map<String, Object> setVersion(final Map<String, Object> before, final long version) {

        final Map<String, Object> copy = new HashMap<>(before);
        Instance.setVersion(copy, version);
        return copy;
    }

    @Test
    protected void testDelete() {

        assumeTrue(supportsDelete());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, setVersion(before, 1L))
                .write().join();

        final Map<String, Object> current = storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join();
        assertNull(current);
        // FIXME
//        if(storage.storageTraits(schema).getHistoryConsistency().isStronger(Consistency.EVENTUAL)) {
//            final Map<String, Object> v1 = storage.readObjectVersion(schema, id, 1L).join();
//            assertNull(v1);
//        }
    }

    @Test
    protected void testLarge() {

        assumeTrue(supportsOversize());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final StringBuilder str = new StringBuilder();
        for(int i = 0; i != 1000000; ++i) {
            str.append("test");
        }

        final Map<String, Object> data = new HashMap<>();
        data.put("string", str.toString());
        final Instance instance = instance(schema, id, 1L, data);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, instance)
                .write().join();

        final BatchResponse results = storage.read(Consistency.ATOMIC)
                .get(schema, id, ImmutableSet.of())
                .read().join();

        assertEquals(1, results.getRefs().size());
    }

    @Test
    protected void testCreateConflict() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Instance after = instance(schema, id, 2L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, after)
                .write().join();

        assertCause(ObjectExistsException.class, () -> storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                    .createObject(schema, id, after)
                    .write().get());
    }

    @Test
    protected void testUpdateMissing() {

        assumeTrue(supportsUpdate());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, setVersion(before, 1L))
                .write().join();

        final Instance after = instance(schema, id, 2L);

        assertCause(VersionMismatchException.class, () -> storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, setVersion(before, 1L), after)
                .write().get());
    }

    @Test
    protected void testDeleteMissing() {

        assumeTrue(supportsDelete());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, setVersion(before, 1L))
                .write().join();

        assertCause(VersionMismatchException.class, () -> storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, setVersion(before, 1L))
                .write().get());
    }

    @Test
    protected void testDeleteWrongVersion() {

        assumeTrue(supportsDelete());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        assumeConcurrentObjectWrite(storage, schema);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());

        final Instance after = instance(schema, id, 2L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, setVersion(before, 1L), after)
                .write().join();

        assertCause(VersionMismatchException.class, () -> storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, setVersion(before, 1L))
                .write().get());
    }

    @Test
    protected void testUpdateWrongVersion() {

        assumeTrue(supportsUpdate());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        assumeConcurrentObjectWrite(storage, schema);

        final String id = UUID.randomUUID().toString();

        final Instance init = instance(schema, id, 1L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, init)
                .write().join();

        final Instance before = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());

        final Instance after = instance(schema, id, 2L);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, setVersion(before, 1L), after)
                .write().join();

//        storage.write(Consistency.ATOMIC)
//                .updateObject(schema, id, 1L, before, after)
//                .commit().join();

        assertCause(VersionMismatchException.class, () -> storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, setVersion(before, 1L), after)
                .write().get());
    }

    @Test
    protected void testMultiValueIndex() {

        assumeTrue(supportsIndexes());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(POINTSET);

        createComplete(storage, schema, ImmutableMap.of(
                "points", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 100L)),
                        new Instance(ImmutableMap.of("x", 5L, "y", 10L))
                )
        ));

        createComplete(storage, schema, ImmutableMap.of(
                "points", ImmutableList.of(
                        new Instance(ImmutableMap.of("x", 10L, "y", 10L)),
                        new Instance(ImmutableMap.of("x", 1L, "y", 10L))
                )
        ));

        final List<Sort> sort = ImmutableList.of(Sort.asc(Name.of(ObjectSchema.ID)));
        final Expression expr = Expression.parse("p.x == 10 && p.y == 100 for any p of points");
        final Page<Map<String, Object>> results = storage.query(Consistency.ATOMIC, schema, expr, Collections.emptyList(), Collections.emptySet()).page(100).join();
        assertEquals(1, results.size());
    }

    @Test
    protected void testNullBeforeUpdate() {

        assumeTrue(supportsUpdate());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, instance(schema, id, 1L))
                .write().join();

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .updateObject(schema, id, null, instance(schema, id, 2L))
                .write().join();
    }

    @Test
    protected void testNullBeforeDelete() {

        assumeTrue(supportsDelete());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SIMPLE);

        final String id = UUID.randomUUID().toString();

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, instance(schema, id, 1L))
                .write().join();

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(schema, id, null)
                .write().join();
    }

    @Test
    protected void testLike() throws IOException {

        assumeTrue(supportsLike());

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(ADDRESS);

        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "london"
        ));
        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "London"
        ));
        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "l%ndon"
        ));
        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "L_ndon"
        ));
        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "L*ndon"
        ));
        createComplete(storage, schema, ImmutableMap.of(
                "country", "United Kingdom",
                "city", "l?ndon"
        ));

        final List<Sort> sort = ImmutableList.of(
                Sort.asc(Name.of("city")),
                Sort.asc(Name.of("zip"))
        );

        assertEquals(6, page(storage, schema, Expression.parse("country == 'United Kingdom' && city ILIKE 'l%'"), sort, 10).size());
        assertEquals(3, page(storage, schema, Expression.parse("country == 'United Kingdom' && city LIKE 'l%'"), sort, 10).size());
        assertEquals(1, page(storage, schema, Expression.parse("country == 'United Kingdom' && city LIKE 'l\\\\%n_on'"), sort, 10).size());
        assertEquals(1, page(storage, schema, Expression.parse("country == 'United Kingdom' && city LIKE 'L\\\\_n_on'"), sort, 10).size());
        assertEquals(1, page(storage, schema, Expression.parse("country == 'United Kingdom' && city LIKE 'l\\\\?n_on'"), sort, 10).size());
        assertEquals(1, page(storage, schema, Expression.parse("country == 'United Kingdom' && city LIKE 'L\\\\*n_on'"), sort, 10).size());
    }

    protected boolean supportsLike() {

        return false;
    }

    protected boolean supportsAggregation() {

        return false;
    }

    @Test
    protected void testAggregation() throws IOException {

        assumeTrue(supportsAggregation(),
                "Aggregation must be enabled for this test");

        final Storage storage = storage(namespace);

        final ViewSchema schema = namespace.requireViewSchema(ADDRESS_STATS);

        final Multimap<String, Map<String, Object>> addresses = loadAddresses();
        bulkLoad(storage, addresses);

        final List<Sort> sort = ImmutableList.of(Sort.desc(Name.of("country")), Sort.asc(Name.of("state")));
        final Pager<Map<String, Object>> sources = storage.query(Consistency.ATOMIC, schema, Expression.parse("true"), sort, null);

        final Page<Map<String, Object>> results = sources.page(300).join();
        log.debug("Aggregation results: {}", results);

        final FlatEncoding encoding = new FlatEncoding();

        final List<Map<String, Object>> expected = Immutable.transform(
                CsvUtils.read(TestStorage.class, "/data/Petstore/AddressStats.csv"),
                v -> schema.create(encoding.decode(v))
        );

        assertEquals(expected, results.getPage());
//        assertEquals(100, results.size());
//        final FlatEncoding encoding = new FlatEncoding();
//
//
//        final List<Map<String, Object>> flat = results.map(encoding::encode).getPage();
//        try(final Writer writer = new FileWriter("aggregates.csv")) {
//            CsvUtils.write(writer, flat);
//        }
    }

    @Test
    protected void testRefIndex() {

        assumeTrue(supportsIndexes());

        final Storage storage = storage(namespace);

        final ObjectSchema target = namespace.requireObjectSchema(REF_TARGET);
        final ObjectSchema source = namespace.requireObjectSchema(REF_SOURCE);

        final Set<Name> expand = Name.parseSet("target");

        assumeConcurrentObjectWrite(storage, target);
        assumeConcurrentObjectWrite(storage, source);

        final String targetId = createComplete(storage, target, ImmutableMap.of());
        createComplete(storage, source, ImmutableMap.of(
                "target", new Instance(ImmutableMap.of(ObjectSchema.ID, targetId))
        ));

        final List<Sort> sort = Sort.parseList("id");
        final Page<Map<String, Object>> page = page(storage, source, Expression.parse("target.id == '" + targetId + "'"), sort, 10);
        assertEquals(1, page.size());
    }

    @Test
    protected void testRefExpandQuery() {

        final Storage storage = storage(namespace);

        final ObjectSchema target = namespace.requireObjectSchema(REF_TARGET);
        final ObjectSchema source = namespace.requireObjectSchema(REF_SOURCE);

        final Set<Name> expand = Name.parseSet("target");

        assumeConcurrentObjectWrite(storage, target);
        assumeConcurrentObjectWrite(storage, source);
        assumeTrue(storage.supportedExpand(source, expand).containsAll(expand));

        final String targetId = createComplete(storage, target, ImmutableMap.of(
                "hello", "world"
        ));
        createComplete(storage, source, ImmutableMap.of(
                "target", new Instance(ImmutableMap.of(ObjectSchema.ID, targetId))
        ));

        final List<Sort> sort = Sort.parseList("id");
        final Page<Map<String, Object>> page = page(storage, source, Expression.parse("target.hello == 'world'"), sort, 10);
        assertEquals(1, page.size());
    }

    @Test
    protected void testRefDeepExpandQuery() {

        final Storage storage = storage(namespace);

        final ObjectSchema target = namespace.requireObjectSchema(REF_TARGET);
        final ObjectSchema source = namespace.requireObjectSchema(REF_SOURCE);

        final Set<Name> expand = Name.parseSet("target.source");

        assumeConcurrentObjectWrite(storage, target);
        assumeConcurrentObjectWrite(storage, source);
        assumeTrue(storage.supportedExpand(source, expand).containsAll(expand));

        final String sourceId = createComplete(storage, source, ImmutableMap.of(
                "hello", "pluto"
        ));
        final String targetId = createComplete(storage, target, ImmutableMap.of(
                "hello", "world",
                "source", ReferableSchema.ref(sourceId)
        ));
        createComplete(storage, source, ImmutableMap.of(
                "target", ReferableSchema.ref(targetId)
        ));

        final List<Sort> sort = Sort.parseList("id");
        final Page<Map<String, Object>> page = page(storage, source, Expression.parse("target.source.hello == 'pluto'"), sort, 10);
        assertEquals(1, page.size());
    }

    @Test
    protected void testDateSort() {

        assumeTrue(supportsIndexes());

        final Storage storage = storage(namespace);

        final ObjectSchema dateSort = namespace.requireObjectSchema(DATE_SORT);

        for(int i = 0; i != 10; ++i) {
            createComplete(storage, dateSort, ImmutableMap.of(
                    "grp", "test"
            ));
        }

        final List<Sort> sort = Sort.parseList("created", "id");
        final Expression expression = Expression.parse("grp == 'test' && created >= '2020-08-01T09:33:00.000Z' && created <= '2050-08-01T09:33:00.000Z'");
        final Page<Map<String, Object>> page = page(storage, dateSort, expression, sort, 5);
        page.forEach(item -> assertTrue(expression.evaluatePredicate(Context.init(item))));
        assertEquals(5, page.size());
    }

    protected boolean supportsRepair() {

        return false;
    }

//    @Test
//    protected void testExpand() throws IOException {
//
//        final Storage storage = storage(namespace);
//
//        final ObjectSchema target = namespace.requireObjectSchema(REF_TARGET);
//        final ObjectSchema expanded = namespace.requireObjectSchema(EXPANDED);
//
//        assumeConcurrentObjectWrite(storage, target);
//        assumeConcurrentObjectWrite(storage, expanded);
//
//        final String targetId = createComplete(storage, target, ImmutableMap.of(
//                "hello", "world"
//        ));
//        createComplete(storage, expanded, ImmutableMap.of(
//                "target", ReferableSchema.ref(targetId)
//        ));
//
//        final List<Sort> sort = Sort.parseList("id");
//        final Page<Map<String, Object>> page = page(storage, expanded, Expression.parse("target.hello == 'world'"), sort, 10);
//        assertEquals(1, page.size());
//    }

//    @Test
//    protected void testRepair() throws IOException {
//
//        assumeTrue(supportsRepair());
//
//        final Storage storage = storage(namespace);
//
//        final ObjectSchema expanded = namespace.requireObjectSchema(EXPANDED);
//        final ObjectSchema refSource = namespace.requireObjectSchema(REF_SOURCE);
//        final ObjectSchema refTarget = namespace.requireObjectSchema(REF_TARGET);
//
//        final String id = UUID.randomUUID().toString();
//
//        final Instance before = instance(expanded, id, 1L, ImmutableMap.of(
//                "target", instance(refTarget, id, 1L, ImmutableMap.of(
//                        "hello", "world"
//                )),
//                "source", instance(refSource, id, 1L, ImmutableMap.of(
//                        "hello", "pluto"
//                ))
//        ));
//
//        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
//                .createObject(expanded, id, before).write().join();
//
//        final List<Sort> sort = Collections.singletonList(Sort.asc(ObjectSchema.ID_NAME));
//        final Set<Name> expand = Name.parseSet("target", "source");
//        final Expression beforeExpression = Expression.parse("target.hello == 'world' && source.hello == 'pluto'");
//        final Expression afterExpression = Expression.parse("target.hello == 'pluto' && source.hello == 'world'");
//
//        // Index is async, and that isn't handled by storage, so expecting zero results
//        assertEquals(0, page(storage, expanded, beforeExpression, sort, expand, 10).size());
//
//        storage.repair(expanded).forEach(source -> {
//            source.page(50, null, null).join();
//        });
//
//        // Repair should put index back into correct state
//        assertEquals(1, page(storage, expanded, beforeExpression, sort, expand, 10).size());
//
//        final Instance after = instance(expanded, id, 2L, ImmutableMap.of(
//                "target", instance(refTarget, id, 2L, ImmutableMap.of(
//                        "hello", "pluto"
//                )),
//                "source", instance(refSource, id, 2L, ImmutableMap.of(
//                        "hello", "world"
//                ))
//        ));
//
//        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
//                .updateObject(expanded, id, before, after).write().join();
//
//        storage.repair(expanded).forEach(source -> {
//            source.page(50, null, null).join();
//        });
//
//        // Should remove the invalid index record and update the incorrect one
//        assertEquals(0, page(storage, expanded, beforeExpression, sort, expand, 10).size());
//        assertEquals(1, page(storage, expanded, afterExpression, sort, expand, 10).size());
//    }

    @Test
    protected void testVersionedRef() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(VERSIONED_REF);

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of(
                "versionedRef", ReferableSchema.versionedRef(id, 1L)
        );

        final Instance after = instance(schema, id, 1L, data);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, after)
                .write().join();

        final Map<String, Object> current = schema.create(storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());
        assertNotNull(current);
        assertEquals(1, Instance.getVersion(current));
    }

    private Page<Map<String, Object>> page(final Storage storage, final ObjectSchema schema, final Expression expression, final List<Sort> sort, final int count) {

        return page(storage, schema, expression, sort, Collections.emptySet(), count);
    }

    private Page<Map<String, Object>> page(final Storage storage, final ObjectSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand, final int count) {

        return storage.query(Consistency.ATOMIC, schema, expression.bind(Context.init()), sort, expand).page(count).join();
    }

    private String createComplete(final Storage storage, final ObjectSchema schema, final Map<String, Object> data) {

        return createComplete(storage, schema, UUID.randomUUID().toString(), data);
    }

    private String createComplete(final Storage storage, final ObjectSchema schema, final String id, final Map<String, Object> data) {

        final StorageTraits traits = storage.storageTraits(schema);
        final Map<String, Object> instance = instance(schema, id, 1L, data);
        final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC, Versioning.CHECKED);
        write.createObject(schema, id, instance);
        for(final Index index : schema.getIndexes().values()) {
            final Consistency best = traits.getIndexConsistency(index.isMultiValue());
            if(index.getConsistency(best).isAsync() && write instanceof DefaultIndexStorage.WriteTransaction) {
                final Map<Index.Key, Map<String, Object>> records = index.readValues(instance);
                records.forEach((key, projection) -> ((DefaultIndexStorage.WriteTransaction)write).createIndex(schema, index, id, 0L, key, projection));
            }
        }

        write.write().join();
        return id;
    }

    private Instance instance(final ObjectSchema schema, final String id, final long version) {

        return instance(schema, id, version, Collections.emptyMap());
    }

    private Instance instance(final ObjectSchema schema, final String id, final long version, final Map<String, Object> data) {

        final Instant now = ISO8601.now();
        final Map<String, Object> instance = new HashMap<>(data);
        Instance.setId(instance, id);
        Instance.setVersion(instance, version);
        Instance.setSchema(instance, schema.getQualifiedName());
        Instance.setCreated(instance, now);
        Instance.setUpdated(instance, now);
        Instance.setHash(instance, schema.hash(instance));
        return schema.create(instance, schema.getExpand(), false);
    }

    private static void assertCause(final Class<? extends Throwable> except, final Executable exe) {

        boolean thrown = true;
        try {
            exe.execute();
            thrown = false;
        } catch (final Throwable t) {
            assertThrows(except, () -> {
                if (t.getCause() == null) {
                    throw t;
                } else {
                    throw t.getCause();
                }
            });
        }
        if(!thrown) {
            // Should have thrown
            assertThrows(except, () -> {
            });
        }
    }

    @Test
    protected void testPolymorphicCreate() {

        assumeTrue(supportsPolymorphism());

        final Storage storage = storage(namespace);

        final InterfaceSchema interfaceSchema = namespace.requireInterfaceSchema(INTERFACE);

        final ObjectSchema extendASchema = namespace.requireObjectSchema(EXTEND_A);
        final ObjectSchema extendBSchema = namespace.requireObjectSchema(EXTEND_B);

        final String idA = createComplete(storage, extendASchema, ImmutableMap.of(
                "propA", "test1"
        ));

        final String idB = createComplete(storage, extendBSchema, ImmutableMap.of(
                "propB", "test2"
        ));

        final Map<String, Object> getA = storage.get(Consistency.ATOMIC, interfaceSchema, idA, ImmutableSet.of()).join();
        final Map<String, Object> getB = storage.get(Consistency.ATOMIC, interfaceSchema, idB, ImmutableSet.of()).join();

        assertNotNull(getA);
        assertNotNull(getB);
        assertEquals("test1", Instance.get(getA, "propA", String.class));
        assertEquals("test2", Instance.get(getB, "propB", String.class));

        assertCause(ObjectExistsException.class, () -> {
            createComplete(storage, extendASchema, idB, ImmutableMap.of(
                    "propA", "test1"
            ));
        });
    }

    @Test
    protected void testPolymorphicDelete() {

        assumeTrue(supportsPolymorphism());
        assumeTrue(supportsDelete());

        final Storage storage = storage(namespace);

        final InterfaceSchema interfaceSchema = namespace.requireInterfaceSchema(INTERFACE);

        final ObjectSchema extendASchema = namespace.requireObjectSchema(EXTEND_A);

        final String idA = createComplete(storage, extendASchema, ImmutableMap.of(
                "propA", "test1"
        ));

        final Map<String, Object> getA = storage.get(Consistency.ATOMIC, interfaceSchema, idA, ImmutableSet.of()).join();
        assertNotNull(getA);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .deleteObject(extendASchema, idA, getA)
                .write().join();

        assertNull(storage.get(Consistency.ATOMIC, interfaceSchema, idA, ImmutableSet.of()).join());
    }

    protected boolean supportsPolymorphism() {

        return true;
    }

    @Test
    void testSecrets() {

        final Storage storage = storage(namespace);

        final ObjectSchema schema = namespace.requireObjectSchema(SECRET);

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = ImmutableMap.of(
                "secret", Secret.encrypted(new byte[]{1, 2, 3})
        );

        final Instance after = instance(schema, id, 1L, data);

        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                .createObject(schema, id, after)
                .write().join();
    }

    @Test
    void testEmptyRead() {

        final Storage storage = storage(namespace);
        final BatchResponse response = storage.read(Consistency.ATOMIC).read().join();
        assertEquals(0, response.getRefs().size());
    }

    @Test
    void testEmptyWrite() {

        final Storage storage = storage(namespace);
        final BatchResponse response = storage.write(Consistency.ATOMIC, Versioning.UNCHECKED).write().join();
        assertEquals(0, response.getRefs().size());
    }

    private static void assumeConcurrentObjectWrite(final Storage storage, final ObjectSchema schema) {

        assumeTrue(storage.storageTraits(schema).getObjectConcurrency().isEnabled(),
                "Object concurrency must be enabled for this test");
    }
}
