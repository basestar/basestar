package io.basestar.expression.text;

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

import io.basestar.expression.Expression;
import lombok.Data;

/**
 * Like
 *
 * Case-sensitive wildcard match.
 *
 * The supported syntax is as-per standard SQL with ESCAPE \
 *
 * For example, % matches any number of characters and _ matches any single character
 */

@Data
public class SLike implements Like {

    public static final String TOKEN = "LIKE";

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs LIKE rhs
     *
     * @param lhs string Left hand operand
     * @param rhs string Right hand operand
     */

    public SLike(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isCaseSensitive() {

        return true;
    }

    @Override
    public Like create(final Expression lhs, final Expression rhs) {

        return new SLike(lhs, rhs);
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public String toString() {

        return lhs + " " + TOKEN + " " + rhs;
    }
}
