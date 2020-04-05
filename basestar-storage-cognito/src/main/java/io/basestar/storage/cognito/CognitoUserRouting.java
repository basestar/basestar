package io.basestar.storage.cognito;

import io.basestar.schema.ObjectSchema;

public interface CognitoUserRouting {

    String getUserPoolId(ObjectSchema schema);
}
