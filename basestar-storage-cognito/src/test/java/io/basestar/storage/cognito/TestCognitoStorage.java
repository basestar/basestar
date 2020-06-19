package io.basestar.storage.cognito;

/*-
 * #%L
 * basestar-storage-cognito
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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.type.Values;
import io.basestar.schema.*;
import io.basestar.schema.use.UseNamed;
import io.basestar.schema.use.UseString;
import io.basestar.storage.Storage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class TestCognitoStorage {

    protected Namespace namespace() {

        return Namespace.builder()
                .setSchema("User", ObjectSchema.builder()
                        .setProperty("test", Property.builder().setType(UseString.DEFAULT))
                        .setProperty("organization", Property.builder().setType(UseNamed.from("Organization")))
                )
                .setSchema("Organization", ObjectSchema.builder()
                        .setProperty("test", Property.builder().setType(UseString.DEFAULT))
                )
                .build();
    }

    protected Storage storage() {

        return CognitoUserStorage.builder()
                .setClient(CognitoIdentityProviderAsyncClient.builder().region(Region.of("us-east-1")).build())
                .setRouting(schema -> "us-east-1_rhBrmRi8n")
                .build();
    }

    @Test
    public void testCreate() {

        final Storage storage = storage();

        final ObjectSchema schema = namespace().requireObjectSchema("User");

        final String id = UUID.randomUUID().toString();

        final Map<String, Object> data = new HashMap<>();
        Instance.setId(data, id);
        Instance.setVersion(data, 1L);
        data.put("test", "value1");
        data.put("organization", ImmutableMap.of(
                "id", "org1"
        ));
        final Instance before = schema.create(data);


        storage.write(Consistency.ATOMIC)
                .createObject(schema, id, before)
                .commit().join();

        assertValid(before, storage.readObject(schema, id).join());

        data.put("test", "value2");
        Instance.setVersion(data, 2L);
        data.put("organization", ImmutableMap.of(
                "id", "org2"
        ));
        final Instance after = schema.create(data);

        storage.write(Consistency.ATOMIC)
                .updateObject(schema, id, before, after)
                .commit().join();

        assertValid(after, storage.readObject(schema, id).join());
    }

    private void assertValid(final Instance expected, final Map<String, Object> actual) {

        assertNotNull(actual);
        assertEquals("User", Instance.getSchema(actual));
        assertNotNull(Instance.getCreated(actual));
        assertNotNull(Instance.getUpdated(actual));
        expected.forEach((k, v) -> {
            final Object v2 = actual.get(k);
            // Database layer will add this
            if(!"hash".equals(k)) {
                assertTrue(Values.equals(v, v2), v + " != " + v2);
            }
        });
    }

    @Test
    public void testGetMissing() {

        final Storage storage = storage();

        final ObjectSchema schema = namespace().requireObjectSchema("User");

        final String id = UUID.randomUUID().toString();

        storage.readObject(schema, id).join();
    }
}
