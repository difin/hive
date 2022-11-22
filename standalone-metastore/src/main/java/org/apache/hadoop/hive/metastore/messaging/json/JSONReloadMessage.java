package org.apache.hadoop.hive.metastore.messaging.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.ReloadMessage;
import org.apache.thrift.TException;

import java.util.Iterator;

/**
 * JSON implementation of JSONReloadMessage
 */
public class JSONReloadMessage extends ReloadMessage {
    @JsonProperty
    private Long timestamp;

    @JsonProperty
    private String server, servicePrincipal, db, table, tableObjJson, ptnObjJson, refreshEvent;

    /**
     * Default constructor, needed for Jackson.
     */
    public JSONReloadMessage() {
    }

    public JSONReloadMessage(String server, String servicePrincipal, Table tableObj, Partition ptnObj,
                             boolean refreshEvent, Long timestamp) {
        this.server = server;
        this.servicePrincipal = servicePrincipal;

        if (null == tableObj) {
            throw new IllegalArgumentException("Table not valid.");
        }

        this.db = tableObj.getDbName();
        this.table = tableObj.getTableName();

        try {
            this.tableObjJson = MessageBuilder.createTableObjJson(tableObj);
            if (null != ptnObj) {
                this.ptnObjJson = MessageBuilder.createPartitionObjJson(ptnObj);
            } else {
                this.ptnObjJson = null;
            }
        } catch (TException e) {
            throw new IllegalArgumentException("Could not serialize: ", e);
        }

        this.timestamp = timestamp;
        this.refreshEvent = Boolean.toString(refreshEvent);

        checkValid();
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getServer() {
        return server;
    }

    @Override
    public String getServicePrincipal() {
        return servicePrincipal;
    }

    @Override
    public String getDB() {
        return db;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isRefreshEvent() { return Boolean.parseBoolean(refreshEvent); }

    @Override
    public Table getTableObj() throws Exception {
        return (Table) MessageBuilder.getTObj(tableObjJson,Table.class);
    }

    @Override
    public Partition getPtnObj() throws Exception {
        return ((null == ptnObjJson) ? null : (Partition) MessageBuilder.getTObj(ptnObjJson, Partition.class));
    }

    @Override
    public String toString() {
        try {
            return JSONMessageDeserializer.mapper.writeValueAsString(this);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Could not serialize: ", exception);
        }
    }
}
