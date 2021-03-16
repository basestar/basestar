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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCognitoGroupStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        final Map<String, Map<String, GroupType>> userPools = new HashMap<>();

        final CognitoIdentityProviderAsyncClient client = mock(CognitoIdentityProviderAsyncClient.class);

        when(client.createGroup(any(CreateGroupRequest.class)))
                .then(args -> {
                    final CreateGroupRequest req = args.getArgumentAt(0, CreateGroupRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.groupName();
                        final GroupType group = group(req);
                        final Map<String, GroupType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        if(userPool.containsKey(id)) {
                            throw GroupExistsException.builder().build();
                        } else {
                            userPool.put(id, group);
                            return CreateGroupResponse.builder()
                                    .group(group)
                                    .build();
                        }
                    });
                });

        when(client.updateGroup(any(UpdateGroupRequest.class)))
                .then(args -> {
                    final UpdateGroupRequest req = args.getArgumentAt(0, UpdateGroupRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.groupName();
                        final Map<String, GroupType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        final GroupType user = userPool.get(id);
                        if(user == null) {
                            throw ResourceNotFoundException.builder().build();
                        } else {
                            userPool.put(id, user.toBuilder()
                                    .build());
                            return UpdateGroupResponse.builder()
                                    .build();
                        }
                    });
                });

        when(client.getGroup(any(GetGroupRequest.class)))
                .then(args -> {
                    final GetGroupRequest req = args.getArgumentAt(0, GetGroupRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.groupName();
                        final Map<String, GroupType> userPool = userPools.getOrDefault(userPoolId, ImmutableMap.of());
                        final GroupType group = userPool.get(id);
                        if(group == null) {
                            throw ResourceNotFoundException.builder().build();
                        } else {
                            return GetGroupResponse.builder()
                                    .group(group)
                                    .build();
                        }
                    });
                });

        when(client.deleteGroup(any(DeleteGroupRequest.class)))
                .then(args -> {
                    final DeleteGroupRequest req = args.getArgumentAt(0, DeleteGroupRequest.class);
                    return CompletableFuture.supplyAsync(() -> {
                        final String userPoolId = req.userPoolId();
                        final String id = req.groupName();
                        final Map<String, GroupType> userPool = userPools.computeIfAbsent(userPoolId, k -> new HashMap<>());
                        if(userPool.containsKey(id)) {
                            userPool.remove(id);
                            return DeleteGroupResponse.builder().build();
                        } else {
                            throw ResourceNotFoundException.builder().build();
                        }
                    });
                });

        return new SplitLayerStorage(new SplitIndexStorage(CognitoGroupStorage.builder()
                .setClient(client)
                .setStrategy(schema -> schema.getQualifiedName().toString())
                .build(), MemoryStorage.builder().build()), MemoryStorage.builder().build());
    }

    private GroupType group(final CreateGroupRequest req) {

        return GroupType.builder()
                .groupName(req.groupName())
                .creationDate(ISO8601.now())
                .lastModifiedDate(ISO8601.now())
                .build();
    }

    @Override
    protected void testCreate() {

        // skip (custom attributes not supported)
    }

    @Override
    protected void testUpdate() {

        // skip (custom attributes not supported)
    }

    @Override
    protected void testEmptyString() {

        // skip (custom attributes not supported)
    }

    @Override
    protected void testPolymorphicCreate() {

        // skip (custom attributes not supported)
    }
}
