package io.basestar.storage.view;

import io.basestar.expression.aggregate.Aggregate;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AggStage implements ViewStage {

    private final List<String> group;

    private final Map<String, Aggregate> aggregates;
}
