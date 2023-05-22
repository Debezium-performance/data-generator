package io.debezium.perf.load.data.scenarios;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class LinearScenarioExecutor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinearScenarioExecutor.class);
    private int delta;

    /// Meaning - pause bewe
    private int rate;

    public LinearScenarioExecutor(int delta, int rate) {
        this.delta = delta;
        this.rate = rate;
    }

    @Override
    public Integer executeScenario(List<HttpRequest> requestList) {

        int requestCounter = 0;
        int expectedAmount = delta;
        HttpClient client = HttpClient.newHttpClient();
        List<CompletableFuture<Void>> requests = new ArrayList<>();
        while (requestCounter < requestList.size()) {
            LOGGER.info(String.format("Sending %s requests in this batch", expectedAmount));
            for (int i = 0; i < expectedAmount && requestCounter < requestList.size(); i++) {
                    requests.add(client.sendAsync(requestList.get(requestCounter), HttpResponse.BodyHandlers.ofString())
                                    .thenApply(response -> {
                                        LOGGER.info("Status code: " + response.statusCode());
                                        return response;
                                    })
                                    .orTimeout(rate, TimeUnit.SECONDS)
                                    .thenApply(HttpResponse::body)
                                    .thenAccept(LOGGER::debug)
                                    .whenComplete((msg, ex) -> {
                                        if (ex != null) {
                                            LOGGER.error("Found exception during send: " + ex.getMessage());
                                        } else {
                                            LOGGER.debug("Request was sent successfully");
                                        }
                                    }));
                requestCounter++;
            }
            expectedAmount += delta;
            if (requestCounter < requestList.size()) {
                try {
                    LOGGER.info("Waiting " + rate + " until next request");
                    Thread.sleep(rate);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
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
        LOGGER.info("Waiting for all futures to finish");
        return requestCounter;
    }
}
