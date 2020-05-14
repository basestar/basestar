package io.basestar.database.event;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.Ref;
import io.basestar.util.PagingToken;
import lombok.Data;
import lombok.experimental.Accessors;


@Data
@Accessors(chain = true)
public class RefQueryEvent implements RefEvent {

    private Ref ref;

    private String schema;

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonDeserialize(using = ExpressionDeseriaizer.class)
    private Expression expression;

    private PagingToken paging;

    public static RefQueryEvent of(final Ref ref, final String schema, final Expression expression) {

        return of(ref, schema, expression, null);
    }

    public static RefQueryEvent of(final Ref ref, final String schema, final Expression expression, final PagingToken paging) {

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
