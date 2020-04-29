package io.basestar.jackson.conf;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsParser;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class SpringLikeJavaPropsFactory extends JavaPropsFactory {

    private final SpringLikeTextProcessor textProcessor = new SpringLikeTextProcessor();

    @Override
    public JavaPropsParser createParser(final Properties props) {

        return super.createParser(_processProperties(props));
    }

    @Override
    protected Properties _loadProperties(final Reader r0, final IOContext ctxt) throws IOException {

        return _processProperties(super._loadProperties(r0, ctxt));
    }

    private Properties _processProperties(final Properties props) {

        final Properties processed = new Properties();
        props.forEach((k, v) -> {
            if(v instanceof String) {
                processed.put(k, textProcessor.apply((String)v));
            } else {
                processed.put(k, v);
            }
        });
        return processed;
    }
}
