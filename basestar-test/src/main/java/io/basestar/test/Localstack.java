package io.basestar.test;

/*-
 * #%L
 * basestar-test
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

import java.util.regex.Pattern;

public class Localstack {

    private static final String ENDPOINT_PREFIX = "http://localhost:";

    private static final int DDB_PORT = 8000;//4566;

    private static final int LOCALSTACK_PORT = 4566;

    public static final String S3_ENDPOINT = ENDPOINT_PREFIX + LOCALSTACK_PORT;

    public static final String DDB_ENDPOINT = ENDPOINT_PREFIX + DDB_PORT;

    public static final String SQS_ENDPOINT = ENDPOINT_PREFIX + LOCALSTACK_PORT;

    public static final String SNS_ENDPOINT = ENDPOINT_PREFIX + LOCALSTACK_PORT;

    public static final String KINESIS_ENDPOINT = ENDPOINT_PREFIX + LOCALSTACK_PORT;

    public static final String SFN_ENDPOINT = ENDPOINT_PREFIX + LOCALSTACK_PORT;

    public static void startDynamoDB() {

        // Localstack's Dynamodb is total crap - fails and requires a restart after a handful of requests.
        TestContainers.ensure(ContainerSpec.builder()
                .image("amazon/dynamodb-local:latest")
                .port(DDB_PORT)
                .waitFor(Pattern.compile("CorsParams:.*"))
                .build()).join();
    }

    public static void start() {

        startDynamoDB();

        TestContainers.ensure(ContainerSpec.builder()
                .image("localstack/localstack:latest")
                .env("SERVICES=s3,sqs,sns,stepfunctions")
                .env("DEFAULT_REGION=us-east-1")
                .port(LOCALSTACK_PORT)
                .waitFor(Pattern.compile("Ready\\."))
                .build()).join();
    }
}
