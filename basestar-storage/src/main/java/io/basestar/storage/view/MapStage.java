package io.basestar.storage.view;

import io.basestar.expression.Expression;
import lombok.Data;

import java.util.Map;

@Data
public class MapStage implements ViewStage {

    private final Map<String, Expression> outputs;
}
