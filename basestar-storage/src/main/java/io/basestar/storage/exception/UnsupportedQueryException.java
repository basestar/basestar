package io.basestar.storage.exception;

/*-
 * #%L
 * basestar-storage
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

import io.basestar.expression.Expression;

public class UnsupportedQueryException extends RuntimeException {

    public UnsupportedQueryException(final String schema, final Expression expression) {

        this(schema, expression, "unspecified");
    }

    public UnsupportedQueryException(final String schema, final Expression expression, final String reason) {

        super("Schema " + schema + " does not support query " + expression + " (reason: " + reason + ")");
    }
}
