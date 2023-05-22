package io.debezium.perf.load.data.scenarios;

import java.net.http.HttpRequest;
import java.util.List;

public interface ScenarioExecutor {
    Integer executeScenario(List<HttpRequest> requestList);
}
