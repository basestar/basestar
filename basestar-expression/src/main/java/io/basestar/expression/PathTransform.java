package io.basestar.expression;

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
