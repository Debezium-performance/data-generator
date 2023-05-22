package io.debezium.perf.load.data;

import io.debezium.perf.load.data.enums.Tables;
import io.debezium.perf.load.json.DmtSchema;
import io.debezium.perf.load.json.DmtTableAttribute;
import net.datafaker.Faker;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PersonDataBuilder implements DataBuilder {
    private static final Random RANDOM = new SecureRandom();
    private static final Tables TABLE = Tables.SPORT;
    Faker dataFaker = new Faker();
    List<DmtSchema> requests;

    public PersonDataBuilder() {
        this.requests = new ArrayList<>();
    }

    @Override
    public DmtSchema generateDataRow(Integer idPool, Tables table) {
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

    @Override
    public DataBuilder addRequests(Integer idPool, int count) {
        for (int i = 0; i < count; i++) {
            this.requests.add(this.generateDataRow(idPool, TABLE));
        }
        return this;
    }

    @Override
    public List<DmtSchema> build() {
        return this.requests;
    }
}
