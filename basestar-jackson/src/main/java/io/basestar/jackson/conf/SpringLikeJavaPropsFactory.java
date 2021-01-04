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

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsParser;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;
import java.util.function.Function;

public class SpringLikeJavaPropsFactory extends JavaPropsFactory {

    private final Function<String, String> textProcessor;

    public SpringLikeJavaPropsFactory() {

        this(SpringLikeTextProcessor.DEFAULT);
    }

    public SpringLikeJavaPropsFactory(final Function<String, String> textProcessor) {

        this.textProcessor = textProcessor;
    }

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
