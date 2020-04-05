package io.basestar.schema.change;

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
