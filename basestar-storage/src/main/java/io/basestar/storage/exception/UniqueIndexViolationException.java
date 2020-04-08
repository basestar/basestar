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

import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;

public class UniqueIndexViolationException extends RuntimeException implements HasExceptionMetadata {

    public static final int STATUS = 409;

    public static final String CODE = "UniqueIndexViolation";

    public static final String INDEX = "index";

    private final String index;

    public UniqueIndexViolationException(final String schema, final String id, final String index) {

        super(schema + " with id \"" + id + "\" cannot be created because index values for " + index + " already exist");
        this.index = index;
    }

    @Override
    public ExceptionMetadata getMetadata() {

        return new ExceptionMetadata()
                .setStatus(STATUS)
                .setCode(CODE)
                .setMessage(getMessage())
                .putData(INDEX, index);
    }
}
