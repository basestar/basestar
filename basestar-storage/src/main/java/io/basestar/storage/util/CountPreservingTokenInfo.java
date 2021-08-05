package io.basestar.storage.util;

import io.basestar.util.Page;
import lombok.Data;

@Data
public class CountPreservingTokenInfo {

    private final Long total;
    private final Page.Token token;
}
