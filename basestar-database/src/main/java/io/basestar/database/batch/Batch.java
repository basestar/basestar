package io.basestar.database.batch;

import io.basestar.database.options.CreateOptions;
import io.basestar.database.options.DeleteOptions;
import io.basestar.database.options.UpdateOptions;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
public class Batch {

    private List<Create> create;

    private List<Update> update;

    private List<Delete> delete;

    @Data
    @Accessors(chain = true)
    public static class Create extends CreateOptions {

        private String schema;

        private Map<String, Object> data;
    }

    @Data
    @Accessors(chain = true)
    public static class Update extends UpdateOptions {

        private String schema;

        private Map<String, Object> data;
    }

    @Data
    @Accessors(chain = true)
    public static class Delete extends DeleteOptions {

        private String schema;

        private Map<String, Object> data;
    }

}
