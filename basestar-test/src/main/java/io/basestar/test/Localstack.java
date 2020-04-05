package io.basestar.test;

import java.util.regex.Pattern;

public class Localstack {

    private static final String ENDPOINT_PREFIX = "http://localhost:";

    private static final int S3_PORT = 4572;

    private static final int DDB_PORT = 4569;

    private static final int SQS_PORT = 4576;

    private static final int SNS_PORT = 4575;

    public static final String S3_ENDPOINT = ENDPOINT_PREFIX + S3_PORT;

    public static final String DDB_ENDPOINT = ENDPOINT_PREFIX + DDB_PORT;

    public static final String SQS_ENDPOINT = ENDPOINT_PREFIX + SQS_PORT;

    public static final String SNS_ENDPOINT = ENDPOINT_PREFIX + SNS_PORT;

    public static void start() {

        TestContainers.ensure(ContainerSpec.builder()
                .image("localstack/localstack")
                .env("SERVICES=s3,dynamodb,sqs,sns")
                .port(S3_PORT)
                .port(DDB_PORT)
                .port(SQS_PORT)
                .port(SNS_PORT)
                .waitFor(Pattern.compile("Ready\\."))
                .build()).join();
    }
}
