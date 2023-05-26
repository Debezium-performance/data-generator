package io.debezium.performance.load.data.builder;

import io.debezium.performance.dmt.schema.DatabaseColumnEntry;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.debezium.performance.load.data.DataTypeConvertor;
import io.debezium.performance.load.data.enums.Tables;

import java.util.ArrayList;

public class BeerDataBuilder extends DataBuilder {

    public BeerDataBuilder() {
        super();
        super.table = Tables.BEER;
    }

    @Override
    public DatabaseEntry generateDataRow(Integer idPool) {
        DatabaseEntry schema = createDefaultScheme(RANDOM.nextInt(idPool), table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());


        String name = dataFaker.beer().name();
        String brand = dataFaker.beer().brand();
        String hop = dataFaker.beer().hop();
        String malt = dataFaker.beer().malt();
        String yeast = dataFaker.beer().yeast();
        String style = dataFaker.beer().style();

        schema.getColumnEntries().add(new DatabaseColumnEntry(name, "name",
                DataTypeConvertor.convertDataType(name)));

        schema.getColumnEntries().add(new DatabaseColumnEntry(brand, "brand",
                DataTypeConvertor.convertDataType(brand)));

        schema.getColumnEntries().add(new DatabaseColumnEntry(hop, "hop",
                DataTypeConvertor.convertDataType(hop)));

        schema.getColumnEntries().add(new DatabaseColumnEntry(malt, "malt",
                DataTypeConvertor.convertDataType(malt)));

        schema.getColumnEntries().add(new DatabaseColumnEntry(yeast, "yeast",
                DataTypeConvertor.convertDataType(yeast)));

        schema.getColumnEntries().add(new DatabaseColumnEntry(style, "style",
                DataTypeConvertor.convertDataType(style)));

        normalise(schema);
        return schema;
    }
}
