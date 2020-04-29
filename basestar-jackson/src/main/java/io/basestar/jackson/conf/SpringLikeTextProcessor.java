package io.basestar.jackson.conf;

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
            matcher.appendReplacement(result, rep);
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
