package io.debezium.performance.load.data.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.performance.dmt.schema.DatabaseEntry;
import io.debezium.performance.load.scenarios.ScenarioRequestExecutor;
import io.debezium.performance.load.scenarios.builder.ScenarioBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class RequestBuilder<G extends ScenarioBuilder> {
    private final DataBuilder dataBuilder;
    private final G scenarioBuilder;
    private String endpoint;
    private Integer maximalRowCount;
    private int requestCount;


    public RequestBuilder(DataBuilder dataBuilder, G scenarioBuilder) {
        this.dataBuilder = dataBuilder;
        this.scenarioBuilder = scenarioBuilder;
    }

    public RequestBuilder<G> setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }


    public RequestBuilder<G> setMaxRows(Integer maximalRowCount) {
        this.maximalRowCount = maximalRowCount;
        return this;
    }

    public RequestBuilder<G> setRequestCount(int requestCount) {
        this.requestCount = requestCount;
        return this;
    }

    private List<HttpRequest> generateRequests(List<DatabaseEntry> payloads, int rate) throws URISyntaxException, JsonProcessingException {
        List<HttpRequest> requests = new ArrayList<>();
        for (DatabaseEntry schema : payloads) {

            String serializedRequest = new ObjectMapper().writeValueAsString(schema);

            requests.add(HttpRequest.newBuilder()
                    .uri(new URI(endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(serializedRequest))
                    .version(HttpClient.Version.HTTP_1_1)
                    .timeout(Duration.ofMillis(rate))
                    .build());
        }
        return requests;
    }

    public void buildAndExecute(int rate) throws URISyntaxException, JsonProcessingException {
        List<DatabaseEntry> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        List<HttpRequest> requests = this.generateRequests(payloads, rate);
        scenarioBuilder.prepareScenario(requests).run();
    }

    public ScenarioRequestExecutor buildScenario(int rate) throws URISyntaxException, JsonProcessingException {
        List<DatabaseEntry> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        List<HttpRequest> requests = this.generateRequests(payloads, rate);
        return scenarioBuilder.prepareScenario(requests);
    }

    public List<HttpRequest> build(int rate) throws URISyntaxException, JsonProcessingException {
        List<DatabaseEntry> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        return this.generateRequests(payloads, rate);
    }

    public List<DatabaseEntry> buildPlain() {
        return dataBuilder.addRequests(maximalRowCount, requestCount).build();
    }

}
