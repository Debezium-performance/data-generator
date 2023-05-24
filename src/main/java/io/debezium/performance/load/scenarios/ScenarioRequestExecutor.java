package io.debezium.performance.load.scenarios;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ScenarioRequestExecutor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScenarioRequestExecutor.class);
    List<ScenarioRequest> requestScenario;

    public ScenarioRequestExecutor(List<ScenarioRequest> requestScenario) {
        this.requestScenario = requestScenario;
    }

    @Override
    public void run() {
        HttpClient client = HttpClient.newHttpClient();

        for (ScenarioRequest sr : requestScenario) {
            List<CompletableFuture<Void>> requests = new ArrayList<>();
            LOGGER.info(String.format("Sending %s requests in this batch", sr.getBatchSize()));
            for (HttpRequest r : sr.getRequests()) {
                requests.add(client.sendAsync(r, HttpResponse.BodyHandlers.ofString())
                        .thenApply(response -> {
                            LOGGER.debug("Status code: " + response.statusCode());
                            return response;
                        })
                        //.orTimeout(rate, TimeUnit.SECONDS)
                        .thenApply(HttpResponse::body)
                        .thenAccept(LOGGER::debug)
                        .whenComplete((msg, ex) -> {
                            if (ex != null) {
                                LOGGER.error("Found exception during send: " + ex.getMessage());
                            } else {
                                LOGGER.debug("Request was sent successfully");
                            }
                        }));
            }
            if (sr.getWait() != null) {
                sr.getWait().run();
            }

            LOGGER.debug("Waiting for all futures to finish");
            requests.forEach(r -> {
                if (r != null && !r.isDone()) {
                    try {
                        r.join();
                    } catch (Exception e) {
                        LOGGER.error("Exception found in future.");
                    }
                }
            });
            requests.clear();
        }
    }

    public List<ScenarioRequest> getRequestScenario() {
        return requestScenario;
    }
}
