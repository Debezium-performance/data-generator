package io.debezium.performance.load.data.builder;

import io.debezium.performance.load.data.DataTypeConvertor;
import io.debezium.performance.load.data.enums.Tables;
import io.debezium.performance.load.data.json.DmtSchema;
import io.debezium.performance.load.data.json.DmtTableAttribute;

public class FoodDataBuilder extends DataBuilder{

    public FoodDataBuilder() {
        super();
        super.table = Tables.FOOD;
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool) {
        DmtSchema schema = new DmtSchema();
        createId(schema, RANDOM.nextInt(idPool));
        schema.setTable(table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());

        String dish = dataFaker.food().dish();

        schema.getPayload().add(new DmtTableAttribute("dish",
                DataTypeConvertor.convertDataType(dish), dish));



        int counterI = 0;
        for (; counterI < RANDOM.nextInt(7); counterI++) {
            String ing = dataFaker.food().ingredient();
            schema.getPayload().add(new DmtTableAttribute(String.format("ingredient_%d", counterI),
                    DataTypeConvertor.convertDataType(ing), ing));
        }
        schema.getPayload().add(new DmtTableAttribute("ingredients",
                DataTypeConvertor.convertDataType(counterI), String.valueOf(counterI)));



        int counterV = 0;
        for (; counterV < RANDOM.nextInt(5); counterV++) {
            String ing = dataFaker.food().vegetable();
            schema.getPayload().add(new DmtTableAttribute(String.format("vegetable_%d", counterI),
                    DataTypeConvertor.convertDataType(ing), ing));
        }
        schema.getPayload().add(new DmtTableAttribute("vegetables",
                DataTypeConvertor.convertDataType(counterV), String.valueOf(counterV)));

        int counterS = 0;
        for (; counterS < RANDOM.nextInt(6); counterS++) {
            String ing = dataFaker.food().spice();
            schema.getPayload().add(new DmtTableAttribute(String.format("spice_%d", counterS),
                    DataTypeConvertor.convertDataType(ing), ing));
        }
        schema.getPayload().add(new DmtTableAttribute("spices",
                DataTypeConvertor.convertDataType(counterS), String.valueOf(counterS)));

        schema.normalise();
        return schema;
    }
}
