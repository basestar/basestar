package io.basestar.storage.aws.stepfunctions;

import io.basestar.schema.Namespace;
import io.basestar.schema.ReferableSchema;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.SplitIndexStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import io.basestar.storage.aws.stepfunction.StepFunctionStorage;
import io.basestar.storage.aws.stepfunction.StepFunctionStrategy;
import io.basestar.test.Localstack;
import io.basestar.util.Name;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TestStepFunctionStorage extends TestStorage {

    private static SfnAsyncClient sfn;

    @BeforeAll
    static void startLocalStack() {

        try {
            Localstack.start();
            sfn = SfnAsyncClient.builder()
                    .endpointOverride(URI.create(Localstack.SFN_ENDPOINT))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("local", "stack")
                    ))
                    .build();
        } catch (final Exception e) {
            log.error("Failed to initialize Localstack", e);
            throw e;
        }
    }

    @AfterAll
    static void close() {

        if(sfn != null) {
            sfn.close();
            sfn = null;
        }
    }

    @Override
    protected Storage storage(final Namespace namespace) {

        final Map<Name, String> stateMachineArns = new HashMap<>();
        namespace.forEachObjectSchema((name, schema) -> {
            final CreateStateMachineResponse create = sfn.createStateMachine(CreateStateMachineRequest.builder()
                    .name(schema.getQualifiedName().toString("_"))
                    .definition("{\"StartAt\": \"Wait\", \"States\": {\"Wait\": {\"Type\": \"Wait\", \"Seconds\": 10, \"End\": true}}}")
                    .build()).join();
            stateMachineArns.put(name, create.stateMachineArn());
        });

        final StepFunctionStrategy strategy = new StepFunctionStrategy() {

            @Override
            public String stateMachineArn(final ReferableSchema schema) {

                return stateMachineArns.get(schema.getQualifiedName());
            }

            @Override
            public Storage.EventStrategy eventStrategy(final ReferableSchema schema) {

                return Storage.EventStrategy.EMIT;
            }
        };

        final StepFunctionStorage storage = StepFunctionStorage.builder()
                .strategy(strategy)
                .client(sfn)
                .build();

        return new SplitIndexStorage(storage, MemoryStorage.builder().build());
    }

    @Override
    protected boolean supportsPolymorphism() {

        return false;
    }

    @Override
    protected boolean supportsOversize() {

        return false;
    }

    @Override
    protected boolean supportsUpdate() {

        return false;
    }

    @Override
    protected boolean supportsDelete() {

        return false;
    }

    @Override
    protected void testCreateConflict() {

        // A conflict will not be raised on create, because create is idempotent
    }
}
