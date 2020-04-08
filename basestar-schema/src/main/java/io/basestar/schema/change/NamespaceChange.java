package io.basestar.schema.change;

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

import io.basestar.schema.Schema;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NamespaceChange {

    @Data
    class Schemas implements NamespaceChange {

        private Schema add;

        private String rename;

        private List<SchemaChange> change;

        private Boolean delete;
    }

    @Data
    class ChangeSchemas implements NamespaceChange {

        private final Map<String, List<SchemaChange>> schemas;
    }

    @Data
    class DeleteSchemas implements NamespaceChange {

        private final Set<String> schemas;
    }

    @Data
    class RenameSchemas implements NamespaceChange {

        private final Map<String, String> schemas;
    }
}

/*

schemas:
    X:
        rename: Y
        changes:

    Y: delete: true

- change: AddSchemas
  schemas:
    X:
      attributes:

- change: ChangeSchemas
  schemas:
    X:
    - action: AddProperty
- change: RenameSchema
  currentName: X
  newName: x
- change: DeleteSchema
  name: X

 */
