package io.debezium.performance.load.data.json;

import java.util.ArrayList;
import java.util.List;

public class DmtSchema {
    List<String> databases;
    String table;
    String primary;

    //String operation;

    List<DmtTableAttribute> payload;

    public DmtSchema(List<String> databases, String table, String primary, String operation, List<DmtTableAttribute> payload) {
        this.databases = databases;
        this.table = table;
        this.primary = primary;
        //this.operation = operation;
        this.payload = payload;
    }

    public DmtSchema() {
        // Default
        this.databases = List.of("MySQL"/*, "PostgreSQL", "MongoDB"*/);
        this.payload = new ArrayList<>();
    }

    public List<String> getDatabases() {
        return databases;
    }

    public void setDatabases(List<String> databases) {
        this.databases = databases;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPrimary() {
        return primary;
    }

    public void setPrimary(String primary) {
        this.primary = primary;
    }

    /*public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }*/

    public List<DmtTableAttribute> getPayload() {
        return payload;
    }

    public void setPayload(List<DmtTableAttribute> payload) {
        this.payload = payload;
    }

    public void normalise() {
        for (DmtTableAttribute attr : payload) {
            if (attr.dataType.contains("VarChar")) {
                attr.value = attr.value.replace("'", " ");
            }
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DmtSchema dmtSchema = (DmtSchema) o;

        if (getDatabases() != null ? !getDatabases().equals(dmtSchema.getDatabases()) : dmtSchema.getDatabases() != null)
            return false;
        if (getTable() != null ? !getTable().equals(dmtSchema.getTable()) : dmtSchema.getTable() != null) return false;
        if (getPrimary() != null ? !getPrimary().equals(dmtSchema.getPrimary()) : dmtSchema.getPrimary() != null)
            return false;
        return getPayload() != null ? getPayload().equals(dmtSchema.getPayload()) : dmtSchema.getPayload() == null;
    }

    @Override
    public int hashCode() {
        int result = getDatabases() != null ? getDatabases().hashCode() : 0;
        result = 31 * result + (getTable() != null ? getTable().hashCode() : 0);
        result = 31 * result + (getPrimary() != null ? getPrimary().hashCode() : 0);
        result = 31 * result + (getPayload() != null ? getPayload().hashCode() : 0);
        return result;
    }
}
