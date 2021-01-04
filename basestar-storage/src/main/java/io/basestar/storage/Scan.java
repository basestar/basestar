package io.basestar.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface Scan {

    int getSegments();

    Segment segment(int segment);

    interface Segment extends Iterator<Map<String, Object>>, AutoCloseable {

        @Override
        void close() throws IOException;

        static Segment fromIterator(final Iterator<Map<String, Object>> iterator) {

            return new Segment() {
                @Override
                public void close() {

                    // not required
                }

                @Override
                public boolean hasNext() {

                    return iterator.hasNext();
                }

                @Override
                public Map<String, Object> next() {

                    return iterator.next();
                }
            };
        }
    }
}
