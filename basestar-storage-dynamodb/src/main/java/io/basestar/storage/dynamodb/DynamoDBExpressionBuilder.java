package io.basestar.storage.dynamodb;

/*-
 * #%L
 * basestar-storage-dynamodb
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

import com.google.common.base.Joiner;
import io.basestar.storage.query.Range;
import io.basestar.util.Path;
import lombok.Data;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;

@Data
class DynamoDBExpressionBuilder {

    private final Set<Path> matched;

    private final List<String> expr = new ArrayList<>();

    final Map<String, String> names = new HashMap<>();

    final Map<String, AttributeValue> values = new HashMap<>();

    int nameOffset;

    int valueOffset;

    private String name(final Path path) {

        final String str = path.toString();
        for(final Map.Entry<String, String> entry : names.entrySet()) {
            if(entry.getValue().equals(str)) {
                return entry.getKey();
            }
        }
        final String name = "n" + nameOffset;
        ++nameOffset;
        names.put("#" + name, str);
        return "#" + name;
    }

    private String value(final Object value) {

        final AttributeValue attr = DynamoDBUtils.toAttributeValue(value);
        for(final Map.Entry<String, AttributeValue> entry : values.entrySet()) {
            if(entry.getValue().equals(attr)) {
                return entry.getKey();
            }
        }
        final String name = "v" + valueOffset;
        ++valueOffset;
        values.put(":" + name, attr);
        return ":" + name;
    }

    public void and(final Path path, final Range<Object> term) {

        term.visit(new Range.Visitor<Object, Void>() {

            @Override
            public Void visitInvalid() {

                return null;
            }

            @Override
            public Void visitEq(final Object eq) {

                if(!matched.contains(path)) {
                    expr.add(name(path) + " = " + value(eq));
                }
                return null;
            }

            @Override
            public Void visitLt(final Object lt) {

                expr.add(name(path) + " < " + value(lt));
                return null;
            }

            @Override
            public Void visitLte(final Object lte) {

                expr.add(name(path) + " <= " + value(lte));
                return null;
            }

            @Override
            public Void visitGt(final Object gt) {

                expr.add(name(path) + " > " + value(gt));
                return null;
            }

            @Override
            public Void visitGte(final Object gte) {

                expr.add(name(path) + " >= " + value(gte));
                return null;
            }

            @Override
            public Void visitGtLt(final Object gt, final Object lt) {

                visitGt(gt);
                visitLt(lt);
                return null;
            }

            @Override
            public Void visitGtLte(final Object gt, final Object lte) {

                visitGt(gt);
                visitLte(lte);
                return null;
            }

            @Override
            public Void visitGteLt(final Object gte, final Object lt) {

                visitGte(gte);
                visitLt(lt);
                return null;
            }

            @Override
            public Void visitGteLte(final Object gte, final Object lte) {

                visitGte(gte);
                visitLte(lte);
                return null;
            }
        });
    }

    public String getExpression() {

        return expr.isEmpty() ? null : Joiner.on(" AND ").join(expr);
    }
}
