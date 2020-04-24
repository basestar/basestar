package io.basestar.spark;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPartitionedUpsert {

    @Test
    public void testExtractUpsertId() {

        assertEquals("xyz", PartitionedUpsert.extractUpsertId(URI.create("s3://blah/blah/__upsert=xyz")));
        assertEquals("xyz", PartitionedUpsert.extractUpsertId(URI.create("s3://blah/blah/__upsert=xyz/")));
    }
}
