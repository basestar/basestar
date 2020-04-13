package io.basestar.database.link;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.logical.And;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;

import java.util.*;
import java.util.stream.Collectors;

// From a link expression (bound to the 'this' object), calculate the data
// that would need to be set on the link to make it valid. Return an empty map if no useful
// information can be derived. Generally only a combination of == and && can create useful data,
// if an impossible && occurs then an empty map should be returned.

public class LinkDataVisitor implements ExpressionVisitor.Defaulting<Map<String, Object>> {

    public static final LinkDataVisitor INSTANCE = new LinkDataVisitor();

    @Override
    public Map<String, Object> visitDefault(final Expression expression) {

        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(lhs instanceof Constant && rhs instanceof PathConstant) {
            final Path path = ((PathConstant)rhs).getPath();
            final Object value = ((Constant)lhs).getValue();
            return deepSet(path, value);
        } else if(lhs instanceof PathConstant && rhs instanceof Constant) {
            final Path path = ((PathConstant)lhs).getPath();
            final Object value = ((Constant)rhs).getValue();
            return deepSet(path, value);
        } else {
            return visitDefault(expression);
        }
    }

    private Map<String, Object> deepSet(final Path path, final Object value) {

        if(path.size() == 1) {
            return Collections.singletonMap(path.last(), value);
        } else {
            return Collections.singletonMap(path.first(), deepSet(path.withoutFirst(), value));
        }
    }

    @Override
    public Map<String, Object> visitAnd(final And expression) {

        final List<Map<String, Object>> results = new ArrayList<>();
        for(final Expression term : expression.getTerms()) {
            results.add(visit(term));
        }
        return merge(results);
    }

    // Merge is quite complicated because it needs to correctly ignore
    // bad-ands (e.g. x == 1 && x == 2) and properly merge deep paths
    // (e.g. x.a == 1 && x.b.c == 2)

    @SuppressWarnings("unchecked")
    private Map<String, Object> merge(final List<Map<String, Object>> input) {

        final Set<String> keys = input.stream().flatMap(v -> v.keySet().stream())
                .collect(Collectors.toSet());

        final Map<String, Object> results = new HashMap<>();
        for(final String key : keys) {
            final List<?> values = input.stream().filter(v -> v.containsKey(key))
                    .map(v -> v.get(key)).collect(Collectors.toList());
            if(!values.isEmpty()) {
                final Object first = values.get(0);
                if(values.stream().skip(1).allMatch(v -> Values.equals(first, v))) {
                    results.put(key, first);
                } else if(values.stream().allMatch(v -> v instanceof Map)){
                    results.put(key, merge((List<Map<String, Object>>)values));
                }
            }
        }
        return results;
    }
}
