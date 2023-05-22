package io.debezium.perf.load.data.scenarios;

import java.net.http.HttpRequest;
import java.util.List;

public interface ScenarioBuilder {
    ScenarioRequestExecutor prepareScenario(List<HttpRequest> requestList);
}
