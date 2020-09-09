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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.api.API;
import io.basestar.api.AuthenticatingAPI;
import io.basestar.api.DiscoverableAPI;
import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.basestar.auth.StaticAuthenticator;
import io.basestar.connector.undertow.UndertowConnector;
import io.basestar.database.DatabaseServer;
import io.basestar.database.api.DatabaseAPI;
import io.basestar.database.options.ReadOptions;
import io.basestar.event.Pump;
import io.basestar.event.sns.SNSEmitter;
import io.basestar.event.sqs.SQSReceiver;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBStrategy;
import io.basestar.storage.dynamodb.DynamoDBUtils;
import io.basestar.storage.s3.S3Stash;
import io.basestar.util.Name;
import io.basestar.util.Page;
import org.apache.commons.lang.StringUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

//import io.basestar.database.storage.LoggingStorage;

public class Test {

    public static void main(final String[] args) throws ExecutionException, InterruptedException, IOException {

        final S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4572"))
                .build();

        final DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4569"))
                .build();

        final String storageOversizeBucket = UUID.randomUUID().toString();
        s3.createBucket(CreateBucketRequest.builder().bucket(storageOversizeBucket).build()).join();

        final SqsAsyncClient sqs = SqsAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4576"))
                .build();

        final String queueName = UUID.randomUUID().toString();
        final String queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueName).build()).join().queueUrl();

        final SnsAsyncClient sns = SnsAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4575"))
                .build();

        final String topicName = UUID.randomUUID().toString();
        final String topicArn = sns.createTopic(CreateTopicRequest.builder()
                .name(topicName).build()).join().topicArn();

        final Map<String, String> attrs = ImmutableMap.of("RawMessageDelivery", "true");

        sns.subscribe(SubscribeRequest.builder().topicArn(topicArn)
                .protocol("sqs").endpoint(queueUrl).attributes(attrs).build()).join();

        final String eventOversizeBucket = UUID.randomUUID().toString();
        s3.createBucket(CreateBucketRequest.builder().bucket(eventOversizeBucket).build()).join();

        ///

        final S3Stash storageOversize = S3Stash.builder()
                .setClient(s3).setBucket(storageOversizeBucket).build();

        final DynamoDBStrategy ddbRouting = DynamoDBStrategy.SingleTable.builder().tablePrefix(UUID.randomUUID() + "-").build();
        final Storage storage = DynamoDBStorage.builder().setClient(ddb).setStrategy(ddbRouting)
                .setOversizeStash(storageOversize).setEventStrategy(Storage.EventStrategy.EMIT)
                .build();

        for(final TableDescription table : ddbRouting.tables(Collections.emptyList()).values()) {
            ddb.createTable(DynamoDBUtils.createTableRequest(table)).join();
        }

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

        ///

        final String[] resources = {
                "/schema/project.yml",
                "/schema/task.yml",
                "/schema/team.yml",
                "/schema/user.yml"
        };

        final Namespace namespace = Namespace.load(Arrays.stream(resources).map(Test.class::getResource).toArray(URL[]::new));
        final DatabaseServer database = new DatabaseServer(namespace, storage, emitter);

        final Pump pump = Pump.create(receiver, database,2, 10);
        pump.start();

        final Caller caller = new Caller() {

            @Override
            public boolean isAnon() {
                return false;
            }

            @Override
            public boolean isSuper() {
                return false;
            }

            @Override
            public Name getSchema() {

                return Name.of("User");
            }

            @Override
            public String getId() {

                return "test";
            }

            @Override
            public Map<String, Object> getClaims() {

                return ImmutableMap.of();
            }
        };

        if(false) {
            final Map<String, Object> createUser = database.create(caller, Name.of("User"), "test", ImmutableMap.of(
                    "owner", ImmutableMap.of(
                            "id", "test"
                    ),
                    "id", "test",
                    "email", "matt@arcneon.com"
            )).get();
            System.err.println(createUser);

            final Map<String, Object> createTeam = database.create(caller, Name.of("Team"), "test", ImmutableMap.of(
                    "id", "test",
                    "owner", ImmutableMap.of(
                            "id", "test"
                    )
            )).get();
            System.err.println(createTeam);

            final Map<String, Object> getTeam = database.read(caller, ReadOptions.builder().schema(Name.of("Team")).id("test")
                    .expand(Name.parseSet("owner")).build()).get();
            System.err.println(getTeam);

            final Map<String, Object> createTeamInvite = database.create(caller, Name.of("TeamInvite"), ImmutableMap.of(
                    "team", ImmutableMap.of(
                            "id", "test"
                    ), "user", ImmutableMap.of(
                            "id", "test"
                    ),
                    "permissions", ImmutableSet.of("UPDATE")
            )).get();
            System.err.println(createTeamInvite);

            final Page<Instance> queryInvites = database.query(caller, Name.of("TeamInvite"), Expression.parse("user.id == 'test'")).get();
            System.err.println(queryInvites);

            final Map<String, Object> createTeamMember = database.create(caller, Name.of("TeamUser"), ImmutableMap.of(
                    "team", ImmutableMap.of(
                            "id", "test"
                    ), "user", ImmutableMap.of(
                            "id", "test"
                    ),
                    "permissions", ImmutableSet.of("UPDATE")
            )).get();
            System.err.println(createTeamMember);

            final Map<String, Object> createProject = database.create(caller, Name.of("Project"), "myproject", ImmutableMap.of(
                    "name", "myproject",
                    "description", "desc",
                    "owner", ImmutableMap.of(
                            "id", "test"
                    )
            )).get();
            System.err.println(createProject);
            final Map<String, Object> updateProject = database.update(caller, Name.of("Project"), "myproject", createProject).get();
            System.err.println(updateProject);

            final Map<String, Object> get = database.read(caller, ReadOptions.builder().schema(Name.of("Project")).id("myproject")
                    .expand(Name.parseSet("owner")).build()).get();
            System.err.println(get);

            final Map<String, Object> getVersion = database.read(caller, Name.of("Project"), "myproject", 1L).get();
            System.err.println(getVersion);

            final Page<Instance> queryProject = database.query(caller, Name.of("Project"), Expression.parse("owner.id == 'test'")).get();

            System.err.println(queryProject);

            final String big = StringUtils.repeat("test", 40000);

            database.create(caller, Name.of("Project"), "myproject2", ImmutableMap.of(
                    "name", "myproject2",
                    "description", big,
                    "owner", ImmutableMap.of(
                            "id", "test"
                    )
            )).get();

            final Map<String, Object> readBig = database.read(caller, ReadOptions.builder().schema(Name.of("Project"))
                    .id("myproject2").expand(Name.parseSet("owner")).build()).get();
            System.err.println(readBig);
        }

        final Authenticator authenticator = new StaticAuthenticator(Caller.SUPER);

//        final API api = new AuthenticatingAPI(authenticator, new DatabaseAPI(database));
//        final OpenAPI openAPI = api.openApi();
        final API api = new AuthenticatingAPI(authenticator, DiscoverableAPI.builder().api(new DatabaseAPI(database)).build());
        //System.err.println(new ObjectMapper().setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY).writerWithDefaultPrettyPrinter().writeValueAsString(openAPI));
        final UndertowConnector connector = UndertowConnector.builder().api(api).host("localhost").port(5004).build();
        connector.start();
    }
}
