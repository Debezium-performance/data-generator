package io.debezium.performance.load.data.builder;

import io.debezium.performance.load.data.DataTypeConvertor;
import io.debezium.performance.load.data.enums.Tables;
import io.debezium.performance.load.data.json.DmtSchema;
import io.debezium.performance.load.data.json.DmtTableAttribute;

public class AviationDataBuilder extends DataBuilder {

    public AviationDataBuilder() {
        super();
        super.table = Tables.AVIATION;
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool) {
        DmtSchema schema = new DmtSchema();
        createId(schema, RANDOM.nextInt(idPool));
        schema.setTable(table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());


        String aircraft = dataFaker.aviation().aircraft();
        String airline = dataFaker.aviation().airline();
        Integer passengers = RANDOM.nextInt(374);
        String airport = dataFaker.aviation().airport();
        String flight = dataFaker.aviation().flight();
        String metar = dataFaker.aviation().METAR();
        Double flightDistance = RANDOM.nextDouble(4000.0);



        schema.getPayload().add(new DmtTableAttribute("aircraft",
                DataTypeConvertor.convertDataType(aircraft), aircraft));

        schema.getPayload().add(new DmtTableAttribute("airline",
                DataTypeConvertor.convertDataType(airline), airline));

        schema.getPayload().add(new DmtTableAttribute("passengers",
                DataTypeConvertor.convertDataType(passengers), passengers.toString()));

        schema.getPayload().add(new DmtTableAttribute("airport",
                DataTypeConvertor.convertDataType(airport), airport));

        schema.getPayload().add(new DmtTableAttribute("flight",
                DataTypeConvertor.convertDataType(flight), flight));

        schema.getPayload().add(new DmtTableAttribute("metar",
                DataTypeConvertor.convertDataType(metar), metar));

        schema.getPayload().add(new DmtTableAttribute("flight_distance",
                DataTypeConvertor.convertDataType(flightDistance), flightDistance.toString()));

        schema.normalise();
        return schema;
    }
}
