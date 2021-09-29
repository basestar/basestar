package io.basestar.stream;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@implementation")
public interface SubscriptionMetadata extends Serializable {

}
