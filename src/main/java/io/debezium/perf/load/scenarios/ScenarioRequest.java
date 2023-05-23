package io.debezium.perf.load.scenarios;

import java.net.http.HttpRequest;
import java.util.List;

public class ScenarioRequest {
    private List<HttpRequest> requests;
    private Runnable wait;

    private final int batchSize;

    public ScenarioRequest(List<HttpRequest> requests, Runnable wait) {
        this.requests = requests;
        this.batchSize = requests.size();
        this.wait = wait;
    }

    public List<HttpRequest> getRequests() {
        return requests;
    }

    public void setRequests(List<HttpRequest> requests) {
        this.requests = requests;
    }

    public Runnable getWait() {
        return wait;
    }

    public void setWait(Runnable wait) {
        this.wait = wait;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
