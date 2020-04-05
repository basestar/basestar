package io.basestar.database.options;

import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Set;

@Data
@Accessors(chain = true)
public class QueryOptions {

    public static final int DEFAULT_COUNT = 10;

    public static final int MAX_COUNT = 50;

    private Integer count;

    private List<Sort> sort;

    private Set<Path> expand;

    private Set<Path> projection;

    private PagingToken paging;
}
