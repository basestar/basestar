package io.basestar.schema.aggregate;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(name = Sum.TYPE, value = Sum.class),
        @JsonSubTypes.Type(name = Min.TYPE, value = Min.class),
        @JsonSubTypes.Type(name = Max.TYPE, value = Max.class),
        @JsonSubTypes.Type(name = Avg.TYPE, value = Avg.class),
        @JsonSubTypes.Type(name = Count.TYPE, value = Count.class)
})
public interface Aggregate {

    <T> T visit(AggregateVisitor<T> visitor);
}
