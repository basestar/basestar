package io.basestar.schema.exception;

/*-
 * #%L
 * basestar-schema
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

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;
import io.basestar.util.Name;

public class MissingSchemaException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 400;

    public static final String CODE = "MissingSchema";

    public static final String SCHEMA = "schema";

    private final Name schema;

    public MissingSchemaException(final Name schema) {

        super("Schema " + schema + " not found");
        this.schema = schema;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(SCHEMA, schema);
    }
}
