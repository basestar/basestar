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
import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.basestar.connector.undertow.UndertowConnector;
import io.basestar.database.DatabaseServer;
import io.basestar.database.api.DatabaseAPI;
import io.basestar.database.options.CreateOptions;
import io.basestar.database.options.QueryOptions;
import io.basestar.database.options.ReadOptions;
import io.basestar.database.options.UpdateOptions;
import io.basestar.event.Pump;
import io.basestar.event.sns.SNSEmitter;
import io.basestar.event.sqs.SQSReceiver;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.storage.Storage;
import io.basestar.storage.dynamodb.DynamoDBRouting;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBUtils;
import io.basestar.storage.s3.S3Stash;
import io.basestar.util.PagedList;
import io.basestar.util.Path;
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

        final DynamoDBRouting ddbRouting = new DynamoDBRouting.SingleTable(UUID.randomUUID() + "-");
        final Storage storage = DynamoDBStorage.builder().setClient(ddb).setRouting(ddbRouting)
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
            public String getSchema() {

                return "User";
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

        final Map<String, Object> createUser = database.create(caller, "User", "test", ImmutableMap.of(
                "owner", ImmutableMap.of(
                        "id", "test"
                ),
                "id", "test",
                "email", "matt@arcneon.com"
        ), new CreateOptions()).get();
        System.err.println(createUser);

        final Map<String, Object> createTeam = database.create(caller, "Team", "test", ImmutableMap.of(
                "id", "test",
                "owner", ImmutableMap.of(
                        "id", "test"
                )
        ), new CreateOptions()).get();
        System.err.println(createTeam);

        final Map<String, Object> getTeam = database.read(caller, "Team", "test", new ReadOptions().setExpand(Path.parseSet("owner"))).get();
        System.err.println(getTeam);

        final Map<String, Object> createTeamInvite = database.create(caller, "TeamInvite", ImmutableMap.of(
                "team", ImmutableMap.of(
                        "id", "test"
                ),"user", ImmutableMap.of(
                        "id", "test"
                ),
                "permissions", ImmutableSet.of("UPDATE")
        ), new CreateOptions()).get();
        System.err.println(createTeamInvite);

        final PagedList<Instance> queryInvites = database.query(caller, "TeamInvite", Expression.parse("user.id == 'test'"), new QueryOptions()).get();
        System.err.println(queryInvites);

        final Map<String, Object> createTeamMember = database.create(caller, "TeamUser", ImmutableMap.of(
                "team", ImmutableMap.of(
                        "id", "test"
                ),"user", ImmutableMap.of(
                        "id", "test"
                ),
                "permissions", ImmutableSet.of("UPDATE")
        ), new CreateOptions()).get();
        System.err.println(createTeamMember);

        final Map<String, Object> createProject = database.create(caller, "Project", "myproject", ImmutableMap.of(
                "name", "myproject",
                "description", "desc",
                "owner", ImmutableMap.of(
                        "id", "test"
                )
        ), new CreateOptions()).get();
        System.err.println(createProject);
        final Map<String, Object> updateProject = database.update(caller, "Project", "myproject", createProject, new UpdateOptions()).get();
        System.err.println(updateProject);

        final Map<String, Object> get = database.read(caller, "Project", "myproject", new ReadOptions().setExpand(Path.parseSet("owner"))).get();
        System.err.println(get);

        final Map<String, Object> getVersion = database.read(caller, "Project", "myproject", new ReadOptions().setVersion(1L)).get();
        System.err.println(getVersion);

        final PagedList<Instance> queryProject = database.query(caller, "Project", Expression.parse("owner.id == 'test'"), new QueryOptions()).get();

        System.err.println(queryProject);

        final String big = StringUtils.repeat("test", 40000);

        database.create(caller, "Project", "myproject2", ImmutableMap.of(
                "name", "myproject2",
                "description", big,
                "owner", ImmutableMap.of(
                        "id", "test"
                )
        ), new CreateOptions()).get();

        final Map<String, Object> readBig = database.read(caller, "Project", "myproject2", new ReadOptions().setExpand(Path.parseSet("owner"))).get();
        System.err.println(readBig);

        final Authenticator authenticator = new Authenticator() {

            @Override
            public String getName() {
                return "None";
            }

            @Override
            public Caller authenticate(final String token) {

                return caller;
            }

            @Override
            public Map<String, Object> openApiScheme() {
                return ImmutableMap.of();
            }

            @Override
            public Caller anon() {

                return caller;
            }

            @Override
            public Caller superuser() {

                return caller;
            }
        };

        final API api = new DatabaseAPI(authenticator, database);
        final UndertowConnector connector = new UndertowConnector(api,"localhost", 5004);
        connector.start();
    }
}
