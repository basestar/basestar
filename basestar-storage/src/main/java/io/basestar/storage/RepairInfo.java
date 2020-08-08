package io.basestar.storage;

import lombok.Data;
import lombok.With;

@Data
@With
public class RepairInfo {

    public static RepairInfo ZERO = new RepairInfo(0, 0, 0, 0, 0);

    private final int scannedObjects;

    private final int scannedIndexRecords;

    private final int deletedIndexRecords;

    private final int createdIndexRecords;

    private final int updatedIndexRecords;

    public static RepairInfo sum(final RepairInfo a, final RepairInfo b) {

        return new RepairInfo(
                a.scannedObjects + b.scannedObjects,
                a.scannedIndexRecords + b.scannedIndexRecords,
                a.deletedIndexRecords + b.deletedIndexRecords,
                a.createdIndexRecords + b.createdIndexRecords,
                a.updatedIndexRecords + b.updatedIndexRecords
        );
    }
}
