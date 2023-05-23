package io.debezium.perf.load.data.builder;

import io.debezium.perf.load.data.DataTypeConvertor;
import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.data.json.DmtSchema;
import io.debezium.perf.load.data.json.DmtTableAttribute;

public class BeerDataBuilder extends DataBuilder {

    public BeerDataBuilder() {
        super();
        super.table = Tables.BEER;
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool) {
        DmtSchema schema = new DmtSchema();
        createId(schema, RANDOM.nextInt(idPool));
        schema.setTable(table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());


        String name = dataFaker.beer().name();
        String brand = dataFaker.beer().brand();
        String hop = dataFaker.beer().hop();
        String malt = dataFaker.beer().malt();
        String yeast = dataFaker.beer().yeast();
        String style = dataFaker.beer().style();

        schema.getPayload().add(new DmtTableAttribute("name",
                DataTypeConvertor.convertDataType(name), name));

        schema.getPayload().add(new DmtTableAttribute("brand",
                DataTypeConvertor.convertDataType(brand), brand));

        schema.getPayload().add(new DmtTableAttribute("hop",
                DataTypeConvertor.convertDataType(hop), hop));

        schema.getPayload().add(new DmtTableAttribute("malt",
                DataTypeConvertor.convertDataType(malt), malt));

        schema.getPayload().add(new DmtTableAttribute("yeast",
                DataTypeConvertor.convertDataType(yeast), yeast));

        schema.getPayload().add(new DmtTableAttribute("style",
                DataTypeConvertor.convertDataType(style), style));

        schema.normalise();
        return schema;
    }
}
