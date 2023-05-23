package io.debezium.perf.load.data.builder;

import io.debezium.perf.load.data.DataTypeConvertor;
import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.data.json.DmtSchema;
import io.debezium.perf.load.data.json.DmtTableAttribute;
import net.datafaker.Faker;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class DataBuilder {
    protected static final Random RANDOM = new SecureRandom();
    protected Tables table = Tables.PERSON;
    protected Faker dataFaker = new Faker();
    protected List<DmtSchema> requests;

    public DataBuilder() {
        this.requests = new ArrayList<>();
    }

    public abstract DmtSchema generateDataRow(Integer idPool);

    DataBuilder addRequests(Integer idPool, int count) {
        for (int i = 0; i < count; i++) {
            this.requests.add(this.generateDataRow(idPool));
        }
        return this;
    }

    List<DmtSchema> build() {
        return this.requests;
    }

    protected void createId(DmtSchema schema, Integer value) {
        schema.setPrimary("id");
        schema.getPayload().add(new DmtTableAttribute("id",
                DataTypeConvertor.convertDataType(value), value.toString()));
    }

    protected  <T extends Enum<?>> T randomEnum(Class<T> clazz, Random random) {
        int x = random.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }


}
