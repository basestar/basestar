package io.basestar.database.event;

import io.basestar.event.Event;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nullable;

@Data
@Accessors(chain = true)
public class RepairEvent implements Event {

    private Name schema;

    @Nullable
    private String index;

    private int source;

    @Nullable
    private Page.Token paging;

    public static RepairEvent of(final Name schema, final int source, final Page.Token paging) {

        return new RepairEvent().setSchema(schema).setSource(source).setPaging(paging);
    }

    public static RepairEvent of(final Name schema, final String index, final int source, final Page.Token paging) {

        return new RepairEvent().setSchema(schema).setIndex(index).setSource(source).setPaging(paging);
    }

    public RepairEvent withPaging(final Page.Token paging) {

        return of(schema, index, source, paging);
    }

    @Override
    public Event abbreviate() {

        return this;
    }
}
