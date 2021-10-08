package io.basestar.spark.hadoop;

import io.basestar.schema.Instance;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.Storage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

public interface WriteAction {

    Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema);

    @Data
    class Create implements WriteAction {

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema) {

            return transaction.createObject((ObjectSchema) schema, Instance.getId(after), after);
        }
    }

    @Data
    class Update implements WriteAction {

        private final Map<String, Object> before;

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema) {

            return transaction.updateObject((ObjectSchema) schema, Instance.getId(after), before, after);
        }
    }

    @Data
    class Delete implements WriteAction {

        private final Map<String, Object> before;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema) {

            return transaction.deleteObject((ObjectSchema) schema, Instance.getId(before), before);
        }
    }

    @Data
    @Slf4j
    class History implements WriteAction {

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema) {

            return transaction.writeHistory((ObjectSchema) schema, Instance.getId(after), after);
        }
    }

    @Data
    @Slf4j
    class Write implements WriteAction {

        private final Map<String, Object> before;

        private final Map<String, Object> after;

        @Override
        public Storage.WriteTransaction apply(final Storage.WriteTransaction transaction, final LinkableSchema schema) {

            return transaction.write(schema, after);
        }
    }
}
