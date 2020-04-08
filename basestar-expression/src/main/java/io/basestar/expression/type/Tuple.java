package io.basestar.expression.type;

/*-
 * #%L
 * basestar-expression
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

import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@Data
public class Tuple implements Iterable<Object> {

    private final List<Object> values;

    public Tuple(final Collection<?> values) {

        this.values = ImmutableList.copyOf(values);
    }

    public Object get(final int i) {

        return values.get(i);
    }

    @Override
    public Iterator<Object> iterator() {

        return values.iterator();
    }
}
