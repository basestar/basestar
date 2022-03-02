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
import io.basestar.schema.Namespace;
import io.basestar.storage.*;
import io.basestar.util.ISO8601;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderAsyncClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCognitoUserStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        final ConcurrentMap<String, Map<String, UserType>> userPools = new ConcurrentHashMap<>();

        final CognitoIdentityProviderAsyncClient client = mock(CognitoIdentityProviderAsyncClient.class);

        when(client.adminCreateUser(any(AdminCreateUserRequest.class)))
                .then(args -> {
                    final AdminCreateUserRequest req = args.getArgument(0, AdminCreateUserRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.username();
                        final UserType user = user(req);
                        final Map<String, UserType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        if(userPool.containsKey(id)) {
                            throw UsernameExistsException.builder().build();
                        } else {
                            userPool.put(id, user);
                            return AdminCreateUserResponse.builder()
                                    .user(user)
                                    .build();
                        }
                    });
                });

        when(client.adminUpdateUserAttributes(any(AdminUpdateUserAttributesRequest.class)))
                .then(args -> {
                    final AdminUpdateUserAttributesRequest req = args.getArgument(0, AdminUpdateUserAttributesRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.username();
                        final Map<String, UserType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        final UserType user = userPool.get(id);
                        if(user == null) {
                            throw UserNotFoundException.builder().build();
                        } else {
                            userPool.put(id, user.toBuilder()
                                    .attributes(req.userAttributes())
                                    .build());
                            return AdminUpdateUserAttributesResponse.builder()
                                    .build();
                        }
                    });
                });

        when(client.adminGetUser(any(AdminGetUserRequest.class)))
                .then(args -> {
                    final AdminGetUserRequest req = args.getArgument(0, AdminGetUserRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.username();
                        final Map<String, UserType> userPool = userPools.getOrDefault(userPoolId, ImmutableMap.of());
                        final UserType user = userPool.get(id);
                        if(user == null) {
                            throw UserNotFoundException.builder().build();
                        } else {
                            return AdminGetUserResponse.builder()
                                    .enabled(user.enabled())
                                    .username(user.username())
                                    .userAttributes(user.attributes())
                                    .userStatus(user.userStatus())
                                    .userCreateDate(user.userCreateDate())
                                    .userLastModifiedDate(user.userLastModifiedDate())
                                    .build();
                        }
                    });
                });

        when(client.adminDeleteUser(any(AdminDeleteUserRequest.class)))
                .then(args -> {
                    final AdminDeleteUserRequest req = args.getArgument(0, AdminDeleteUserRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.username();
                        final Map<String, UserType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        if(userPool.containsKey(id)) {
                            userPool.remove(id);
                            return AdminDeleteUserResponse.builder().build();
                        } else {
                            throw UserNotFoundException.builder().build();
                        }
                    });
                });

        return new SplitLayerStorage(new SplitIndexStorage(CognitoUserStorage.builder()
                .setClient(client)
                .setStrategy(schema -> schema.getQualifiedName().toString())
                .build(), MemoryStorage.builder().build()), MemoryStorage.builder().build());
    }

    private UserType user(final AdminCreateUserRequest req) {

        return UserType.builder()
                .username(req.username())
                .userStatus(UserStatusType.UNCONFIRMED)
                .attributes(req.userAttributes())
                .enabled(true)
                .userCreateDate(ISO8601.now())
                .userLastModifiedDate(ISO8601.now())
                .build();
    }

    @Override
    protected boolean supportsHistoryQuery() {

        return false;
    }

//    @Override
//    protected boolean supportsIndexes() {
//
//        return false;
//    }

    //
//    @Test
//    void testCreate() {
//
//        final Storage storage = storage();
//
//        final ObjectSchema schema = namespace().requireObjectSchema("User");
//
//        final String id = UUID.randomUUID().toString();
//
//        final Map<String, Object> data = new HashMap<>();
//        Instance.setId(data, id);
//        Instance.setVersion(data, 1L);
//        data.put("test", "value1");
//        data.put("organization", ImmutableMap.of(
//                "id", "org1"
//        ));
//        final Instance before = schema.create(data);
//
//
//        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
//                .createObject(schema, id, before)
//                .write().join();
//
//        assertValid(before, storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());
//
//        data.put("test", "value2");
//        Instance.setVersion(data, 2L);
//        data.put("organization", ImmutableMap.of(
//                "id", "org2"
//        ));
//        final Instance after = schema.create(data);
//
//        storage.write(Consistency.ATOMIC, Versioning.CHECKED)
//                .updateObject(schema, id, before, after)
//                .write().join();
//
//        assertValid(after, storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join());
//    }
//
//    private void assertValid(final Instance expected, final Map<String, Object> actual) {
//
//        assertNotNull(actual);
//        assertEquals(Name.of("User"), Instance.getSchema(actual));
//        assertNotNull(Instance.getCreated(actual));
//        assertNotNull(Instance.getUpdated(actual));
//        expected.forEach((k, v) -> {
//            final Object v2 = actual.get(k);
//            // Database layer will add this
//            if(!"hash".equals(k)) {
//                assertTrue(Values.equals(v, v2), v + " != " + v2);
//            }
//        });
//    }
//
//    @Test
//    void testGetMissing() {
//
//        final Storage storage = storage();
//
//        final ObjectSchema schema = namespace().requireObjectSchema("User");
//
//        final String id = UUID.randomUUID().toString();
//
//        storage.get(Consistency.ATOMIC, schema, id, ImmutableSet.of()).join();
//    }
}
