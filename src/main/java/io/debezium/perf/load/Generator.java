package io.debezium.perf.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.perf.load.data.PersonDataBuilder;
import io.debezium.perf.load.data.RequestBuilder;
import io.debezium.perf.load.data.scenarios.LinearScenarioExecutor;

import java.net.URISyntaxException;

public class Generator {

    public static void main(String[] args) throws URISyntaxException, JsonProcessingException {
        int rate = 5000;
        RequestBuilder<PersonDataBuilder, LinearScenarioExecutor> requestBuilder
                = new RequestBuilder<>(new PersonDataBuilder(), new LinearScenarioExecutor(2000, rate));

        requestBuilder
                .setMaxRows(500)
                .setRequestCount(20000)
                .setEndpoint("http://dmt-route-debezium-apps.apps.worker-01.strimzi.app-services-dev.net/Main/CreateTableAndUpsert")
                .setPort(8080)
                .buildAndExecute(rate);

        /*ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(Paths.get("data.json").toFile(), data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }
}
