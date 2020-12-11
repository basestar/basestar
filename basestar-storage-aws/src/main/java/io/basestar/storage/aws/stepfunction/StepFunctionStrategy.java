package io.basestar.storage.aws.stepfunction;

import io.basestar.schema.ReferableSchema;
import io.basestar.storage.Storage;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;

public interface StepFunctionStrategy {

    String DEFAULT_STATUS_PROPERTY = "status";

    String stateMachineArn(ReferableSchema schema);

    Storage.EventStrategy eventStrategy(ReferableSchema schema);

    default String statusProperty(final ReferableSchema schema) {

        return DEFAULT_STATUS_PROPERTY;
    }

    default Object statusValue(final ReferableSchema schema, final ExecutionStatus status) {

        return status.name();
    }
}
