package io.basestar.util;

/*-
 * #%L
 * basestar-core
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import com.google.common.io.BaseEncoding;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

@Data
public class PagingToken implements Serializable {

    private static final BaseEncoding ENCODING = BaseEncoding.base64Url().omitPadding();

    private final byte[] value;

    public PagingToken(final byte[] value) {

        this.value = Arrays.copyOf(value, value.length);
    }

    public PagingToken(final String value) {

        this.value = ENCODING.decode(value);
    }

    @Override
    public String toString() {

        return ENCODING.encode(value);
    }
}
