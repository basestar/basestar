package io.basestar.database.options;

/*-
 * #%L
 * basestar-database
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
@Builder(toBuilder = true, builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = QueryLinkOptions.Builder.class)
public class QueryLinkOptions {

    public static final String TYPE = "queryLink";

    public static final int DEFAULT_COUNT = 10;

    public static final int MAX_COUNT = 50;

    private final Name schema;

    private final String id;

    private final String link;

    private final Expression expression;

    private final Consistency consistency;

    private final Integer count;

    private final List<Sort> sort;

    private final Set<Name> expand;

    private final Set<Name> projection;

    private final Set<Page.Stat> stats;

    private final Page.Token paging;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

    }
}
