package io.basestar.storage.util;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.TestStorage;
import io.basestar.storage.exception.PagingTokenSyntaxException;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestKeysetPagingUtils {

    @Test
    void testDatePaging() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("DateSort");

        final Map<String, Object> object = new HashMap<>();
        Instance.setCreated(object, ISO8601.now());

        KeysetPagingUtils.keysetPagingToken(schema, ImmutableList.of(Sort.asc(Name.of("created"))), object);
    }

    @Test
    void testCountPreservingToken() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("DateSort");

        final Map<String, Object> object = new HashMap<>();
        final Instant now = ISO8601.now();
        Instance.setCreated(object, now);
        final ImmutableList<Sort> sortingInfo = ImmutableList.of(Sort.asc(Name.of("created")));

        final Page.Token originalToken = KeysetPagingUtils.keysetPagingToken(schema, sortingInfo, object);
        final  Page.Token countPreservingToken = KeysetPagingUtils.countPreservingToken(originalToken, 98);
        final CountPreservingTokenInfo countPreservingTokenInfo = KeysetPagingUtils.countPreservingTokenInfo(countPreservingToken);
        final List<Object> sort = KeysetPagingUtils.keysetValues(schema, sortingInfo, countPreservingTokenInfo.getToken());

        assertEquals(98, countPreservingTokenInfo.getTotal());
        assertEquals(originalToken, countPreservingTokenInfo.getToken());
        assertEquals(1, sort.size());
        assertEquals(now, sort.get(0));
    }

    @Test
    void testMismatchingTokenDeserialization() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("DateSort");

        final Map<String, Object> object = new HashMap<>();
        final Instant now = ISO8601.now();
        Instance.setCreated(object, now);
        final ImmutableList<Sort> sortingInfo = ImmutableList.of(Sort.asc(Name.of("created")));

        final Page.Token pagingToken = KeysetPagingUtils.keysetPagingToken(schema, sortingInfo, object);
        assertThrows(PagingTokenSyntaxException.class, () -> KeysetPagingUtils.countPreservingTokenInfo(pagingToken));
    }
}
