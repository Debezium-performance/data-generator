package io.debezium.perf.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.perf.load.data.PersonDataBuilder;
import io.debezium.perf.load.data.RequestBuilder;
import io.debezium.perf.load.data.scenarios.ScenarioRequestExecutor;
import io.debezium.perf.load.data.scenarios.builder.ConstantScenarioBuilder;
import io.debezium.perf.load.data.scenarios.builder.LinearScenarioBuilder;
import io.debezium.perf.load.data.scenarios.builder.PeakScenarioBuilder;

import java.net.URISyntaxException;

public class Generator {

    public static void main(String[] args) throws URISyntaxException, JsonProcessingException {
        int rate = 5000;
        RequestBuilder<PersonDataBuilder, ConstantScenarioBuilder> requestBuilder
                = new RequestBuilder<>(new PersonDataBuilder(), new ConstantScenarioBuilder(50, 5000));

        ScenarioRequestExecutor sc = requestBuilder
                .setMaxRows(500)
                .setRequestCount(5000)
                .setEndpoint("http://dmt-route-debezium-apps.apps.worker-01.strimzi.app-services-dev.net/Main/CreateTableAndUpsert")
                .buildScenario(rate);
        int x = 5;
    }
}
