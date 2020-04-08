//package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
//
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import io.basestar.util.Path;
//import lombok.Data;
//import lombok.experimental.Accessors;
//
//import java.util.Set;
//
//@Data
//@Accessors(chain = true)
//public class Method implements Member {
//
//    @JsonIgnore
//    private String name;
//
//    @Override
//    public void resolve(final String name, final Schema schema, final Namespace namespace) {
//
//        this.name = name;
//    }
//
//    @Override
//    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {
//
//        return null;
//    }
//
//    @Override
//    public Set<Path> requireExpand(final Set<Path> paths) {
//
//        return null;
//    }
//}
