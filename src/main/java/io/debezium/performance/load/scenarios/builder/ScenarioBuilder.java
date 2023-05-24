package io.debezium.performance.load.scenarios.builder;

import io.debezium.performance.load.scenarios.ScenarioRequestExecutor;

import java.net.http.HttpRequest;
import java.util.List;

public interface ScenarioBuilder {
    ScenarioRequestExecutor prepareScenario(List<HttpRequest> requestList);
}
