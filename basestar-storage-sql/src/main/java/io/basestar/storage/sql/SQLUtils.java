package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
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

import io.basestar.util.Sort;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

public class SQLUtils {

    public static SortOrder sort(final Sort.Order order) {

        return order == Sort.Order.ASC ? SortOrder.ASC : SortOrder.DESC;
    }

    public static org.jooq.Name parseName(final String qualifiedName) {

        return DSL.name(qualifiedName.split("\\."));
    }
}
