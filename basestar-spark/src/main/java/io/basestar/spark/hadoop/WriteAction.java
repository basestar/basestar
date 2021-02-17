package io.basestar.spark.hadoop;

import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

public interface WriteAction {

    Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final ObjectSchema schema, final String id);

    @Data
    class Create implements WriteAction {

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final ObjectSchema schema, final String id) {

            return transaction.createObject(schema, id, after);
        }
    }

    @Data
    class Update implements WriteAction {

        private final Map<String, Object> before;

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final ObjectSchema schema, final String id) {

            return transaction.updateObject(schema, id, before, after);
        }
    }

    @Data
    class Delete implements WriteAction {

        private final Map<String, Object> before;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final ObjectSchema schema, final String id) {

            return transaction.deleteObject(schema, id, before);
        }
    }

    @Data
    @Slf4j
    class History implements WriteAction {

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final ObjectSchema schema, final String id) {

            return transaction.writeHistory(schema, id, after);
        }
    }
}
