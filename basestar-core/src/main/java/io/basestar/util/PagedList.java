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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Wither;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@Wither
@EqualsAndHashCode(callSuper = false)
public class PagedList<T> extends AbstractList<T> implements Serializable {

    private final List<T> page;

    private final PagingToken paging;

    public static <T> PagedList<T> empty() {

        return new PagedList<>(Collections.emptyList(), null);
    }

    public boolean hasPaging() {

        return getPaging() != null;
    }

    public <U> PagedList<U> map(final Function<? super T, ? extends U> fn) {

        final PagedList<T> delegate = this;
        final List<U> results = delegate.getPage().stream().map(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging);
    }

    public PagedList<T> filter(final Predicate<? super T> fn) {

        final PagedList<T> delegate = this;
        final List<T> results = delegate.getPage().stream().filter(fn)
                .collect(Collectors.toList());

        return new PagedList<>(results, paging);
    }

    @Override
    public T get(final int index) {

        return page.get(index);
    }

    @Override
    public int size() {

        return page.size();
    }
}
