package io.debezium.perf.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.perf.load.data.PersonDataBuilder;
import io.debezium.perf.load.data.RequestBuilder;
import io.debezium.perf.load.data.scenarios.LinearScenarioBuilder;

import java.net.URISyntaxException;

public class Generator {

    public static void main(String[] args) throws URISyntaxException, JsonProcessingException {
        int rate = 5000;
        RequestBuilder<PersonDataBuilder, LinearScenarioBuilder> requestBuilder
                = new RequestBuilder<>(new PersonDataBuilder(), new LinearScenarioBuilder(2000, rate));

        requestBuilder
                .setMaxRows(500)
                .setRequestCount(5000)
                .setEndpoint("http://dmt-route-debezium-apps.apps.worker-01.strimzi.app-services-dev.net/Main/CreateTableAndUpsert")
                .buildAndExecute(rate);
    }
}
