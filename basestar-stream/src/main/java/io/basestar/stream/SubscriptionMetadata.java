package io.basestar.stream;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@implementation")
public interface SubscriptionMetadata {

}
