package io.basestar.expression.methods;

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

import io.basestar.expression.type.Coercion;
import io.basestar.util.ISO8601;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;

@SuppressWarnings("unused")
public class StringMethods implements Serializable {

    private static final int MAX_PAD_LENGTH = 1048576;

    public boolean isempty(final String target) {

        return target.isEmpty();
    }

    public boolean isblank(final String target) {

        return target.trim().isEmpty();
    }

    public int size(final String target) {

        return target.length();
    }

    public String substr(final String target, final Number begin) {

        return target.substring(begin.intValue());
    }

    public String substr(final String target, final Number begin, final Number end) {

        return target.substring(begin.intValue(), end.intValue());
    }

    public String _substr(final String target, final Number begin) {

        return target.substring(begin.intValue() - 1);
    }

    public String _substr(final String target, final Number begin, final Number count) {

        final int beginPos = begin.intValue() - 1;
        final int endPos = Math.min(target.length(), beginPos + count.intValue());
        return target.substring(beginPos, endPos);
    }

    public String lpad(final String target, final Long length, final String c) {

        if (target == null || length == null || c == null) {
            return null;
        } else if (c.length() != 1) {
            throw new IllegalStateException("LPad only supports a single padding character");
        } else if (length > MAX_PAD_LENGTH) {
            throw new IllegalStateException("LPad would produce a string over " + MAX_PAD_LENGTH + " characters");
        } else {
            if (target.length() >= length) {
                return target;
            } else {
                final int pad = length.intValue() - target.length();
                final StringBuilder buffer = new StringBuilder();
                for (int i = 0; i != pad; ++i) {
                    buffer.append(c);
                }
                buffer.append(target);
                return buffer.toString();
            }
        }
    }

    public String rpad(final String target, final Long length, final String c) {

        if (target == null || length == null || c == null) {
            return null;
        } else if (c.length() != 1) {
            throw new IllegalStateException("RPad only supports a single padding character");
        } else if (length > MAX_PAD_LENGTH) {
            throw new IllegalStateException("RPad would produce a string over " + MAX_PAD_LENGTH + " characters");
        } else {
            if (target.length() >= length) {
                return target;
            } else {
                final int pad = length.intValue() - target.length();
                final StringBuilder buffer = new StringBuilder();
                buffer.append(target);
                for (int i = 0; i != pad; ++i) {
                    buffer.append(c);
                }
                return buffer.toString();
            }
        }
    }

    public String trim(final String target) {

        return target.trim();
    }

    public static String ltrim(final String target) {

        int i = 0;
        while (i < target.length() && Character.isWhitespace(target.charAt(i))) {
            i++;
        }
        return target.substring(i);
    }

    public static String rtrim(final String target) {

        int i = target.length()-1;
        while (i >= 0 && Character.isWhitespace(target.charAt(i))) {
            i--;
        }
        return target.substring(0, i + 1);
    }

    public LocalDate todate(final String value, final String format) {

        return ISO8601.parseDate(value, format);
    }

    public Instant todatetime(final String value, final String format) {

        return ISO8601.parseDateTime(value, format);
    }

    public String _concat(final Object a, final Object b) {

        return Coercion.toString(a) + Coercion.toString(b);
    }

    public String _concat(final Object a, final Object b, final Object c) {

        return Coercion.toString(a) + Coercion.toString(b) + Coercion.toString(c);
    }
}
