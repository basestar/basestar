package io.basestar.storage.util;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.TestStorage;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class TestKeysetPagingUtils {

    @Test
    public void testDatePaging() throws IOException {

        final Namespace namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("DateSort");

        final Map<String, Object> object = new HashMap<>();
        Instance.setCreated(object, LocalDateTime.now());

        KeysetPagingUtils.keysetPagingToken(schema, ImmutableList.of(Sort.asc(Name.of("created"))), object);
    }
}
