package io.basestar.schema.use;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Schema;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.leangen.geantyref.TypeFactory;
import io.swagger.v3.oas.models.media.ArraySchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

@Data
@Slf4j
public class UsePage<T> implements UseCollection<T, Page<T>> {

    public static final String NAME = "page";

    private final Use<T> type;

    public static <T> UsePage<T> from(final Use<T> type) {

        return new UsePage<>(type);
    }

    @Override
    public Page<T> transformValues(final Page<T> value, final BiFunction<Use<T>, T, T> fn) {

        if(value != null) {
            boolean changed = false;
            final List<T> result = new ArrayList<>();
            for(final T before : value) {
                final T after = fn.apply(type, before);
                result.add(after);
                changed = changed || (before != after);
            }
            return changed ? new Page<>(result, value.getPaging(), value.getStats()) : value;
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T2> UsePage<T2> transform(final Function<Use<T>, Use<T2>> fn) {

        final Use<T2> type2 = fn.apply(type);
        if(type2 == type ) {
            return (UsePage<T2>)this;
        } else {
            return new UsePage<>(type2);
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitPage(this);
    }

    @Override
    public Use<?> resolve(final Schema.Resolver resolver) {

        final Use<?> resolved = type.resolve(resolver);
        if(resolved == type) {
            return this;
        } else {
            return new UseArray<>(resolved);
        }
    }

    @Override
    public Page<T> create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createPage(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.PAGE;
    }

    @Override
    public Type javaType(final Name name) {

        if(name.isEmpty()) {
            return TypeFactory.parameterizedClass(Page.class, type.javaType());
        } else {
            return type.javaType(name.withoutFirst());
        }
    }

    @Override
    public Page<T> defaultValue() {

        return Page.empty();
    }

    @Override
    public Object toConfig(final boolean optional) {

        return ImmutableMap.of(
                Use.name(NAME, optional), type
        );
    }

    @Override
    public Page<T> deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static <T> Page<T> deserializeAnyValue(final DataInput in) throws IOException {

        final List<T> result = new ArrayList<>();
        final int size = in.readInt();
        for(int i = 0; i != size; ++i) {
            result.add(Use.deserializeAny(in));
        }
        return new Page<>(result, null);
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new ArraySchema().items(type.openApi(expand));
    }

    @Override
    public boolean areEqual(final Page<T> a, final Page<T> b) {

        if(a == null || b == null) {
            return a == null && b == null;
        } else if(a.size() == b.size()) {
            for(int i = 0; i != a.size(); ++i) {
                if(!type.areEqual(a.get(i), b.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }
}
