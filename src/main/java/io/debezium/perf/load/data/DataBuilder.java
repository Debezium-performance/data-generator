package io.debezium.perf.load.data;

import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.json.DmtSchema;
import io.debezium.perf.load.json.DmtTableAttribute;

import java.util.List;
import java.util.Random;

public interface DataBuilder {
    DmtSchema generateDataRow(Integer idPool, Tables table);

    DataBuilder addRequests(Integer idPool, int count);

    List<DmtSchema> build();


    default void createId(DmtSchema schema, Integer value) {
        schema.setPrimary("id");
        schema.getPayload().add(new DmtTableAttribute("id",
                DataTypeConvertor.convertDataType(value), value.toString()));
    }

    default <T extends Enum<?>> T randomEnum(Class<T> clazz, Random random){
        int x = random.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }


}
