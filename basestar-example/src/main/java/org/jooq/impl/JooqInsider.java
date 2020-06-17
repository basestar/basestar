package org.jooq.impl;

/*-
 * #%L
 * basestar-example
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
