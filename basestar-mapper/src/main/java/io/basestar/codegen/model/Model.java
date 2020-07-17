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

import io.basestar.codegen.CodegenSettings;

import javax.validation.Payload;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.lang.annotation.Annotation;

@SuppressWarnings("unused")
public class Model {

    private final CodegenSettings settings;

    public Model(final CodegenSettings settings) {

        this.settings = settings;
    }

    protected CodegenSettings getSettings() {

        return settings;
    }

    public String getPackageName() {

        return settings.getPackageName();
    }

    protected static final Valid VALID = new Valid() {

        @Override
        public Class<? extends Annotation> annotationType() {

            return Valid.class;
        }
    };

    protected static final NotNull NOT_NULL = new NotNull() {

        @Override
        public String message() {
            return null;
        }

        @Override
        public Class<?>[] groups() {
            return new Class[0];
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<? extends Payload>[] payload() {

            return new Class[0];
        }

        @Override
        public Class<? extends Annotation> annotationType() {

            return NotNull.class;
        }
    };
}
