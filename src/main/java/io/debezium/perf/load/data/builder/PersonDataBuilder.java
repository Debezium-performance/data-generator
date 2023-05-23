package io.debezium.perf.load.data.builder;

import io.debezium.perf.load.data.DataTypeConvertor;
import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.data.json.DmtSchema;
import io.debezium.perf.load.data.json.DmtTableAttribute;

import java.util.ArrayList;

public class PersonDataBuilder extends DataBuilder {

    public PersonDataBuilder() {
        super();
        super.table = Tables.PERSON;
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool) {
        DmtSchema schema = new DmtSchema();
        createId(schema, RANDOM.nextInt(idPool));
        schema.setTable(table.toString().toLowerCase());
        //schema.setOperation(randomEnum(Operations.class, RANDOM).toString().toLowerCase());

        String firstName = dataFaker.name().firstName();
        String lastName = dataFaker.name().lastName();
        // AGE
        Integer age = RANDOM.nextInt(100);
        // Average day computer work
        Double avg = RANDOM.nextDouble();
        String city = dataFaker.address().city();
        String company = dataFaker.company().name();
        String color = dataFaker.color().name();

        schema.getPayload().add(new DmtTableAttribute("name",
                DataTypeConvertor.convertDataType(firstName), firstName));

        schema.getPayload().add(new DmtTableAttribute("lastname",
                DataTypeConvertor.convertDataType(lastName), lastName));

        schema.getPayload().add(new DmtTableAttribute("age",
                DataTypeConvertor.convertDataType(age), age.toString()));

        schema.getPayload().add(new DmtTableAttribute("computeraverage",
                DataTypeConvertor.convertDataType(avg), avg.toString()));

        schema.getPayload().add(new DmtTableAttribute("city",
                DataTypeConvertor.convertDataType(city), city));

        schema.getPayload().add(new DmtTableAttribute("company",
                DataTypeConvertor.convertDataType(company), company));

        schema.getPayload().add(new DmtTableAttribute("favouritecolor",
                DataTypeConvertor.convertDataType(color), color));

        schema.normalise();
        return schema;
    }
}
