package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public abstract class ReloadMessage extends EventMessage {

    protected ReloadMessage() {
        super(EventType.RELOAD);
    }

    /**
     * Get the table object associated with the insert
     *
     * @return The Json format of Table object
     */
    public abstract Table getTableObj() throws Exception;

    /**
     * Get the partition object associated with the insert
     *
     * @return The Json format of Partition object if the table is partitioned else return null.
     */
    public abstract Partition getPtnObj() throws Exception;

    /**
     * Getter for the name of the table being insert into.
     * @return Table-name (String).
     */
    public abstract String getTable();

    /**
     * Getter for the replace flag being insert into/overwrite
     * @return Replace flag to represent INSERT INTO or INSERT OVERWRITE (Boolean).
     */
    public abstract boolean isRefreshEvent();

    @Override
    public EventMessage checkValid() {
        if (getTable() == null)
            throw new IllegalStateException("Table name unset.");
        return super.checkValid();
    }
}
