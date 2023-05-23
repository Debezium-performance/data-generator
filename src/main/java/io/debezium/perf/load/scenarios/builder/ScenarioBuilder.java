package io.debezium.perf.load.scenarios.builder;

import io.debezium.perf.load.scenarios.ScenarioRequestExecutor;

import java.net.http.HttpRequest;
import java.util.List;

public interface ScenarioBuilder {
    ScenarioRequestExecutor prepareScenario(List<HttpRequest> requestList);
}
