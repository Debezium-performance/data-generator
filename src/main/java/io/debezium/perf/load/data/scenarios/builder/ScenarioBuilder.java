package io.debezium.perf.load.data.scenarios.builder;

import io.debezium.perf.load.data.scenarios.ScenarioRequestExecutor;

import java.net.http.HttpRequest;
import java.util.List;

public interface ScenarioBuilder {
    ScenarioRequestExecutor prepareScenario(List<HttpRequest> requestList);
}
