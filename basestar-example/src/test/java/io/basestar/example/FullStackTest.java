package io.basestar.example;

/*-
 * #%L
 * basestar-example
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.api.API;
import io.basestar.api.APIResponse;
import io.basestar.api.AuthenticatingAPI;
import io.basestar.auth.Authenticator;
import io.basestar.database.DatabaseServer;
import io.basestar.database.api.DatabaseAPI;
import io.basestar.event.Pump;
import io.basestar.event.sns.SNSEmitter;
import io.basestar.event.sqs.SQSReceiver;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBStrategy;
import io.basestar.storage.dynamodb.DynamoDBUtils;
import io.basestar.storage.s3.S3Stash;
import io.basestar.test.Localstack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FullStackTest {

    @BeforeAll
    public static void startLocalStack() {

        Localstack.start();
    }

    @Test
    @Disabled
    public void test() throws IOException {

        final S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create(Localstack.S3_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(s3::close));

        final DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(Localstack.DDB_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(ddb::close));

        final SqsAsyncClient sqs = SqsAsyncClient.builder()
                .endpointOverride(URI.create(Localstack.SQS_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(sqs::close));

        final SnsAsyncClient sns = SnsAsyncClient.builder()
                .endpointOverride(URI.create(Localstack.SNS_ENDPOINT))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(sns::close));

        // Create SQS queue

        final String queueName = UUID.randomUUID().toString();
        final String queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueName).build()).join().queueUrl();

        // Create SNS topic

        final String topicName = UUID.randomUUID().toString();
        final String topicArn = sns.createTopic(CreateTopicRequest.builder()
                .name(topicName).build()).join().topicArn();

        // Subscribe queue to topic

        final Map<String, String> attrs = ImmutableMap.of("RawMessageDelivery", "true");

        final SubscribeResponse response = sns.subscribe(SubscribeRequest.builder().topicArn(topicArn)
                .protocol("sqs").endpoint(queueUrl).attributes(attrs).build()).join();

        System.err.println(response.subscriptionArn());

        // Oversize buckets

        final String storageOversizeBucket = UUID.randomUUID().toString();
        s3.createBucket(CreateBucketRequest.builder().bucket(storageOversizeBucket).build()).join();

        final String eventOversizeBucket = UUID.randomUUID().toString();
        s3.createBucket(CreateBucketRequest.builder().bucket(eventOversizeBucket).build()).join();

        // Event emitter/receiver

        final S3Stash eventOversize = S3Stash.builder()
                .setClient(s3).setBucket(eventOversizeBucket).build();

        final SQSReceiver receiver = SQSReceiver.builder()
                .setClient(sqs)
                .setQueueUrl(queueUrl)
                .setOversizeStash(eventOversize)
                .build();

        final SNSEmitter emitter = SNSEmitter.builder()
                .setClient(sns)
                .setTopicArn(topicArn)
                .setOversizeStash(eventOversize)
                .build();

        // Storage configuration

        final S3Stash storageOversize = S3Stash.builder()
                .setClient(s3).setBucket(storageOversizeBucket).build();

        final DynamoDBStrategy ddbRouting = DynamoDBStrategy.SingleTable.builder().tablePrefix(UUID.randomUUID() + "-").build();
        final Storage storage = DynamoDBStorage.builder().setClient(ddb).setStrategy(ddbRouting)
                .setOversizeStash(storageOversize).setEventStrategy(Storage.EventStrategy.EMIT)
                .build();

        // Create tables

        for(final TableDescription table : ddbRouting.tables(Collections.emptyList()).values()) {
            ddb.createTable(DynamoDBUtils.createTableRequest(table)).join();
        }

        // Database

        final Namespace namespace = Namespace.load(FullStackTest.class.getResource("schema.yml"));
        final DatabaseServer database = new DatabaseServer(namespace, storage, emitter);

        //  Event pump

        final Pump pump = Pump.create(receiver, database,2, 10);
        pump.start();

        final Authenticator authenticator = new TestAuthenticator();

        final API api = new AuthenticatingAPI(authenticator, new DatabaseAPI(database));

        final Multimap<String, String> headers = HashMultimap.create();
        headers.put("Authorization", "Explicit user1");

        final APIResponse createUser1 = api.handle(TestRequests.put("User/user1", HashMultimap.create(), headers, ImmutableMap.of(
        ))).join();
        assertEquals(201, createUser1.getStatusCode());

        final APIResponse createUser2 = api.handle(TestRequests.put("User/user2", HashMultimap.create(), headers, ImmutableMap.of(
        ))).join();
        assertEquals(201, createUser2.getStatusCode());

        final APIResponse createGroup1 = api.handle(TestRequests.put("Group/group1", HashMultimap.create(), headers, ImmutableMap.of(
                "owner", ImmutableMap.of(
                        ObjectSchema.ID, "user1"
                ),
                "members", ImmutableList.of(
                        ImmutableMap.of(
                                ObjectSchema.ID, "user1"
                        ),
                        ImmutableMap.of(
                                ObjectSchema.ID, "user2"
                        )
                )
        ))).join();
        assertEquals(201, createGroup1.getStatusCode());
        System.err.println(TestRequests.responseBody(createGroup1, Map.class));

        final APIResponse queryGroups = api.handle(TestRequests.get("Group", HashMultimap.create(), headers)).join();
        assertEquals(200, queryGroups.getStatusCode());
        System.err.println(TestRequests.responseBody(queryGroups, List.class));

        // Cannot create this group because caller != owner
        final APIResponse createGroup2 = api.handle(TestRequests.put("Group/group2", HashMultimap.create(), headers, ImmutableMap.of(
                "owner", ImmutableMap.of(
                        ObjectSchema.ID, "user2"
                )
        ))).join();
        assertEquals(403, createGroup2.getStatusCode());

        //pump.flush();

        final Multimap<String, String> expand = HashMultimap.create();
        expand.put("expand", "groups");
        final APIResponse userGroups = api.handle(TestRequests.get("User/user1", expand, headers)).join();
        assertEquals(200, userGroups.getStatusCode());
        System.err.println(TestRequests.responseBody(userGroups, Map.class));

        final APIResponse createProject1 = api.handle(TestRequests.put("Project/project1", HashMultimap.create(), headers, ImmutableMap.of(
                "owner", ImmutableMap.of(
                        ObjectSchema.ID, "group1"
                )
        ))).join();
        assertEquals(201, createProject1.getStatusCode());
        System.err.println(TestRequests.responseBody(createProject1, Map.class));

        pump.stop();
    }

    private Multimap<String, String> query(final String query) {

        final Multimap<String, String> result = HashMultimap.create();
        result.put("query", query);
        return result;
    }

}
