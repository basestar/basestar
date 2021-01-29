package io.basestar.storage.elasticsearch.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.secret.Secret;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFieldType {

    @Test
    void testMapSecret() {

        final FieldType.MapType mapType = new FieldType.MapType(
                FieldType.SECRET
        );

        final Secret v = Secret.encrypted(new byte[]{1, 2, 3});

        final Object result = mapType.fromSource(ImmutableList.of(
                ImmutableMap.of(
                        "key", "k",
                        "value", v.encryptedBase64()
                )
        ));

        assertEquals(ImmutableMap.of(
                "k", v
        ), result);
    }
}
