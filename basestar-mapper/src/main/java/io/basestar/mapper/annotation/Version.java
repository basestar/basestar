package io.basestar.mapper.annotation;

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

import io.basestar.mapper.internal.MemberMapper;
import io.basestar.mapper.internal.MetadataMapper;
import io.basestar.mapper.internal.annotation.MemberDeclaration;
import io.basestar.type.PropertyContext;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@MemberDeclaration(Version.Declaration.class)
public @interface Version {

    @RequiredArgsConstructor
    class Declaration implements MemberDeclaration.Declaration {

        private final Version annotation;

        @Override
        public MemberMapper<?> mapper(final PropertyContext prop) {

            return new MetadataMapper(MetadataMapper.Name.VERSION, prop);
        }
    }
}
