package org.jooq.impl;

import org.jooq.SelectQuery;
import org.jooq.Table;

public class JooqInsider {

    public Table<?> getFrom(final SelectQuery<?> query) {

        if(query instanceof SelectQueryImpl<?>) {
            final TableList tables = ((SelectQueryImpl<?>) query).getFrom();
            tables.forEach(t -> {
                if(t instanceof JoinTable) {
                    final Table<?> lhs = ((JoinTable) t).lhs;
                    final Table<?> rhs = ((JoinTable) t).rhs;
                } else if(t instanceof TableImpl<?>) {
                    final String name = t.getName();
                } else {

                }
            });
        }
        return null;
    }
}
