package io.debezium.performance.load.data.builder;

import io.debezium.performance.dmt.schema.DatabaseColumnEntry;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.debezium.performance.load.data.DataTypeConvertor;
import io.debezium.performance.load.data.enums.Tables;
import net.datafaker.Faker;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class DataBuilder {
    protected static final Random RANDOM = new SecureRandom();
    protected Tables table = Tables.PERSON;
    protected Faker dataFaker = new Faker();
    protected List<DatabaseEntry> requests;

    public DataBuilder() {
        this.requests = new ArrayList<>();
    }

    public abstract DatabaseEntry generateDataRow(Integer idPool);


    protected void normalise(DatabaseEntry entry) {
        for (int i = 0; i < entry.getColumnEntries().size(); i++) {
            DatabaseColumnEntry attr = entry.getColumnEntries().get(i);
            if (attr.dataType().contains("VarChar")) {
                entry.getColumnEntries().set(i, new DatabaseColumnEntry(attr.value().replace("'", " "), attr.columnName(), attr.dataType()));
            }
        }
    }

    DataBuilder addRequests(Integer idPool, int count) {
        for (int i = 0; i < count; i++) {
            this.requests.add(this.generateDataRow(idPool));
        }
        return this;
    }

    List<DatabaseEntry> build() {
        return this.requests;
    }

    protected void createId(DatabaseEntry schema, Integer value) {
        schema.getDatabaseTable().setPrimary("id");
        schema.getColumnEntries().add(new DatabaseColumnEntry(value.toString(), "id",
                DataTypeConvertor.convertDataType(value)));
    }

    protected <T extends Enum<?>> T randomEnum(Class<T> clazz, Random random) {
        int x = random.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }


}
