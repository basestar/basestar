package io.basestar.storage.cognito;

import io.basestar.schema.ObjectSchema;

public interface CognitoGroupRouting {

    String getUserPoolId(ObjectSchema schema);
}
