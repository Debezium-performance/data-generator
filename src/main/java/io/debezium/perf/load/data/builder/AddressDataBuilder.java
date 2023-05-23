package io.debezium.perf.load.data.builder;

import io.debezium.perf.load.data.DataTypeConvertor;
import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.data.json.DmtSchema;
import io.debezium.perf.load.data.json.DmtTableAttribute;
import net.datafaker.Faker;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

public class AddressDataBuilder extends DataBuilder {

    public AddressDataBuilder() {
        super();
        super.table = Tables.ADDRESS;
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool) {
        DmtSchema schema = new DmtSchema();
        createId(schema, RANDOM.nextInt(idPool));
        schema.setTable(table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());

        String city = dataFaker.address().cityName();
        String cityPref = dataFaker.address().cityPrefix();
        String street = dataFaker.address().streetName();
        Integer streetNumber = Integer.valueOf(dataFaker.address().streetAddressNumber());
        String zipCode = dataFaker.address().zipCode();
        Integer floor = RANDOM.nextInt(72);
        Double latitude = Double.valueOf(dataFaker.address().latitude());
        Double longtitude = Double.valueOf(dataFaker.address().longitude());


        schema.getPayload().add(new DmtTableAttribute("city",
                DataTypeConvertor.convertDataType(city), city));

        schema.getPayload().add(new DmtTableAttribute("city_prefix",
                DataTypeConvertor.convertDataType(cityPref), cityPref));

        schema.getPayload().add(new DmtTableAttribute("street",
                DataTypeConvertor.convertDataType(street), street));

        schema.getPayload().add(new DmtTableAttribute("street_number",
                DataTypeConvertor.convertDataType(streetNumber), streetNumber.toString()));

        schema.getPayload().add(new DmtTableAttribute("zip_code",
                DataTypeConvertor.convertDataType(zipCode), zipCode));

        schema.getPayload().add(new DmtTableAttribute("floor",
                DataTypeConvertor.convertDataType(floor), floor.toString()));

        schema.getPayload().add(new DmtTableAttribute("latitude",
                DataTypeConvertor.convertDataType(latitude), latitude.toString()));

        schema.getPayload().add(new DmtTableAttribute("longtitude",
                DataTypeConvertor.convertDataType(longtitude), longtitude.toString()));

        schema.normalise();
        return schema;
    }
}
