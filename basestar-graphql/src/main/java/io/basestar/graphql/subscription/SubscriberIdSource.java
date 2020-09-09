package io.basestar.graphql.subscription;

import io.basestar.api.APIRequest;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public interface SubscriberIdSource {

    CompletableFuture<String> subscriberId(APIRequest request);

    static SubscriberIdSource fromHeader(final String headerName) {

        return request -> {
            final String value = request.getFirstHeader(headerName);
            if(value == null) {
                throw new IllegalStateException("Subscription request header " + headerName + " missing");
            }
            return CompletableFuture.completedFuture(value);
        };
    }

    static SubscriberIdSource fromHeaders(final String delimiter, final String ... headerNames) {

        return request -> {
            final String values = Arrays.stream(headerNames).map(headerName -> {
                final String value = request.getFirstHeader(headerName);
                if (value == null) {
                    throw new IllegalStateException("Subscription request header " + headerName + " missing");
                }
                return value;
            }).collect(Collectors.joining(delimiter));
            return CompletableFuture.completedFuture(values);
        };
    }
}
