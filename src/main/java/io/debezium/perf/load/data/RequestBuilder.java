package io.debezium.perf.load.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.perf.load.data.scenarios.ScenarioRequestExecutor;
import io.debezium.perf.load.data.scenarios.builder.ScenarioBuilder;
import io.debezium.perf.load.json.DmtSchema;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class RequestBuilder<T extends DataBuilder, G extends ScenarioBuilder> {
    private String endpoint;
    private final T dataBuilder;

    private final G scenarioBuilder;
    private Integer maximalRowCount;
    private int requestCount;


    public RequestBuilder(T dataBuilder, G scenarioBuilder) {
        this.dataBuilder = dataBuilder;
        this.scenarioBuilder = scenarioBuilder;
    }

    public RequestBuilder<T, G> setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }


    public RequestBuilder<T, G> setMaxRows(Integer maximalRowCount) {
        this.maximalRowCount = maximalRowCount;
        return this;
    }

    public RequestBuilder<T, G> setRequestCount(int requestCount) {
        this.requestCount = requestCount;
        return this;
    }

    private List<HttpRequest> generateRequests(List<DmtSchema> payloads, int rate) throws URISyntaxException, JsonProcessingException {
        List<HttpRequest> requests = new ArrayList<>();
        for (DmtSchema schema : payloads) {

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
        List<DmtSchema> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        List<HttpRequest> requests = this.generateRequests(payloads, rate);
        scenarioBuilder.prepareScenario(requests).run();
    }

    public ScenarioRequestExecutor buildScenario(int rate) throws URISyntaxException, JsonProcessingException {
        List<DmtSchema> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        List<HttpRequest> requests = this.generateRequests(payloads, rate);
        return scenarioBuilder.prepareScenario(requests);
    }

    public List<HttpRequest> build(int rate) throws URISyntaxException, JsonProcessingException {
        List<DmtSchema> payloads = dataBuilder.addRequests(maximalRowCount, requestCount).build();
        return this.generateRequests(payloads, rate);
    }

    public List<DmtSchema> buildPlain() {
        return dataBuilder.addRequests(maximalRowCount, requestCount).build();
    }

}
