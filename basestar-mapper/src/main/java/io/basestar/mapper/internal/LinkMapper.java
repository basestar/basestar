package io.basestar.mapper.internal;

import io.basestar.expression.Expression;
import io.basestar.schema.Link;
import io.basestar.schema.ObjectSchema;
import io.basestar.type.PropertyContext;
import io.basestar.util.Sort;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class LinkMapper implements MemberMapper<ObjectSchema.Builder> {

    private final String name;

    private final PropertyContext property;

    private final Expression expression;

    private final List<Sort> sort;

    public LinkMapper(final String name, final PropertyContext property, final Expression expression, final List<Sort> sort) {

        this.name = name;
        this.property = property;
        this.expression = expression;
        this.sort = sort;
    }

    @Override
    public void addToSchema(final ObjectSchema.Builder builder) {

        builder.setLink(name, Link.builder()
                .setExpression(expression)
                .setSort(sort));
    }

    @Override
    public void unmarshall(final Object source, final Map<String, Object> target) throws InvocationTargetException, IllegalAccessException {

    }

    @Override
    public void marshall(final Map<String, Object> source, final Object target) throws InvocationTargetException, IllegalAccessException {

    }
}
