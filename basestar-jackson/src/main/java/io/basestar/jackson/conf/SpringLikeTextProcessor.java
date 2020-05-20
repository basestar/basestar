package io.basestar.jackson.conf;

/*-
 * #%L
 * basestar-jackson
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

import io.basestar.util.Nullsafe;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpringLikeTextProcessor implements Function<String, String> {

    private static final Pattern PATTERN = Pattern.compile("\\$\\{([^:}]+):([^}]+)?}");

    @Override
    public String apply(final String str) {

        final StringBuffer result = new StringBuffer();
        final Matcher matcher = PATTERN.matcher(str);
        while (matcher.find()) {
            final String key = matcher.group(1);
            final String def = matcher.group(2);
            final String rep = lookup(key, def);
            matcher.appendReplacement(result, Nullsafe.option(rep));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    protected String lookup(final String key, final String defaultValue) {

        final String result = System.getenv(key);
        if(result == null) {
            return defaultValue;
        } else {
            return result;
        }
    }
}
