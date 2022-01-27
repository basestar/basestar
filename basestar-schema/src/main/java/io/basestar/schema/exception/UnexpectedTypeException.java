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

import io.basestar.schema.Schema;
import io.basestar.schema.use.Use;

public class UnexpectedTypeException extends RuntimeException {

    public UnexpectedTypeException(final Schema expected, final Object actual) {

        this(expected.getQualifiedName().toString(), actual);
    }

    public UnexpectedTypeException(final Use<?> expected, final Object actual) {

        this(expected.toString(), actual);
    }

    public UnexpectedTypeException(final String expected, final Object actual) {

        super("Expected type " + expected + " but was " + (actual == null ? "(null)" : actual.getClass()));
    }
}
