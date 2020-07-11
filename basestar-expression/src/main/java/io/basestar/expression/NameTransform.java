package io.basestar.expression;

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

import io.basestar.util.Name;

import java.util.Collection;

public interface NameTransform {

    Name transform(Name name);

    static NameTransform noop() {

        return path -> path;
    }

    static NameTransform root(final Name root) {

        return root::with;
    }

    static NameTransform unroot(final Name root) {

        return path -> {
            if(path.isChild(root)) {
                return path.withoutFirst(root.size());
            } else {
                throw new IllegalStateException("Unbound path " + path);
            }
        };
    }

    static NameTransform move(final Name from, final Name to) {

        return path -> {
            if(path.isChild(from)) {
                return to.with(path.withoutFirst(from.size()));
            } else {
                return path;
            }
        };
    }

    static NameTransform closure(final Collection<String> closed, final NameTransform transform) {

        return path -> {

            final String first = path.first();
            if(closed.contains(first)) {
                return path;
            } else {
                return transform.transform(path);
            }
        };
    }
}
