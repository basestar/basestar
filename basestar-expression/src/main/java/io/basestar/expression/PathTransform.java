package io.basestar.expression;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.util.Path;

import java.util.Collection;

public interface PathTransform {

    Path transform(Path path);

    static PathTransform noop() {

        return path -> path;
    }

    static PathTransform root(final Path root) {

        return root::with;
    }

    static PathTransform unroot(final Path root) {

        return path -> {
            if(path.isChild(root)) {
                return path.withoutFirst(root.size());
            } else {
                throw new IllegalStateException("Unbound path " + path);
            }
        };
    }

    static PathTransform move(final Path from, final Path to) {

        return path -> {
            if(path.isChild(from)) {
                return to.with(path.withoutFirst(from.size()));
            } else {
                return path;
            }
        };
    }

    static PathTransform closure(final Collection<String> closed, final PathTransform transform) {

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
