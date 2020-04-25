package io.basestar.schema.use;

import io.basestar.expression.Context;
import io.basestar.schema.Instance;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Schema;
import io.basestar.util.Path;

import java.util.Set;

public interface UseInstance extends UseNamed<Instance> {

    InstanceSchema getSchema();

    @Override
    default String getName() {

        return getSchema().getName();
    }

    @Override
    default UseInstance resolve(final Schema.Resolver resolver) {

        return this;
    }

    @Override
    default Use<?> typeOf(final Path path) {

        if(path.isEmpty()) {
            return this;
        } else {
            return getSchema().typeOf(path);
        }
    }

    @Override
    default Instance applyVisibility(final Context context, final Instance value) {

        if(value == null) {
            return null;
        } else {
            return getSchema().applyVisibility(context, value);
        }
    }

    @Override
    default Instance evaluateTransients(final Context context, final Instance value, final Set<Path> expand) {

        if(value == null) {
            return null;
        } else {
            return getSchema().evaluateTransients(context, value, expand);
        }
    }

    @Override
    default Set<Path> transientExpand(final Path path, final Set<Path> expand) {

        return getSchema().transientExpand(path, expand);
    }
}
