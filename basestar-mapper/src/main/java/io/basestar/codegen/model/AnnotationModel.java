package io.basestar.codegen.model;

/*-
 * #%L
 * basestar-mapper
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

import com.google.common.collect.ImmutableSet;
import io.basestar.codegen.CodegenSettings;
import io.basestar.type.AnnotationContext;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class AnnotationModel<A extends Annotation> extends Model {

    private static final Set<String> SKIP_NAMES = ImmutableSet.of("payload", "groups");

    private final A annotation;

    public AnnotationModel(final CodegenSettings settings, final A annotation) {

        super(settings);
        this.annotation = annotation;
    }

    public String getClassName() {

        return annotation.annotationType().getName();
    }

    public Map<String, Object> getValues() {

        final AnnotationContext<A> context =  new AnnotationContext<>(annotation);
        final Map<String, Object> values = context.nonDefaultValues();

        return values.entrySet().stream().filter(e -> e.getValue() != null && !SKIP_NAMES.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
