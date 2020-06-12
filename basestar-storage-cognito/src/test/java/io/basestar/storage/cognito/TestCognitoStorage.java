package io.basestar.storage.cognito;

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
        Instance.setVersion(data, 0L);
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
//        assertNotNull(Instance.getHash(actual));
        assertEquals(0, Instance.getVersion(actual));
        expected.forEach((k, v) -> {
            final Object v2 = actual.get(k);
            // Database layer will add this
            if(!"hash".equals(k)) {
                assertTrue(Values.equals(v, v2), v + " != " + v2);
            }
        });
    }
}
