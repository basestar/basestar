package io.basestar.database.event;

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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import io.basestar.util.PagingToken;
import lombok.Data;
import lombok.experimental.Accessors;


@Data
@Accessors(chain = true)
public class RefQueryEvent implements RefEvent {

    private Ref ref;

    private Name schema;

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonDeserialize(using = ExpressionDeseriaizer.class)
    private Expression expression;

    private PagingToken paging;

    public static RefQueryEvent of(final Ref ref, final Name schema, final Expression expression) {

        return of(ref, schema, expression, null);
    }

    public static RefQueryEvent of(final Ref ref, final Name schema, final Expression expression, final PagingToken paging) {

        return new RefQueryEvent().setRef(ref).setSchema(schema).setExpression(expression).setPaging(paging);
    }

    public RefQueryEvent withPaging(final PagingToken paging) {

        return of(ref, schema, expression, paging);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}
