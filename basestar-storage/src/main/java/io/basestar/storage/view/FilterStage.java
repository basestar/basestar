package io.basestar.storage.view;

import io.basestar.expression.Expression;
import lombok.Data;

@Data
public class FilterStage implements ViewStage {

    private final Expression filter;
}
