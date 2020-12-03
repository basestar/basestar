package io.basestar.storage.aws.stepfunction;

import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;

public interface StepFunctionStrategy {

    String DEFAULT_STATUS_PROPERTY = "status";

    String stateMachineArn(ObjectSchema schema);

    Storage.EventStrategy eventStrategy(ObjectSchema schema);

    default String statusProperty(final ObjectSchema schema) {

        return DEFAULT_STATUS_PROPERTY;
    }

    default Object statusValue(final ObjectSchema schema, final ExecutionStatus status) {

        return status.name();
    }
}
