package io.debezium.performance.load.scenarios;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScenarioRequestExecutor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScenarioRequestExecutor.class);
    List<ScenarioRequest> requestScenario;

    public ScenarioRequestExecutor(List<ScenarioRequest> requestScenario) {
        this.requestScenario = requestScenario;
    }

    @Override
    public void run() {
        LOGGER.info("I am in run now");

        AtomicInteger numberOfSucc = new AtomicInteger(0);
        AtomicInteger numberOfFailed = new AtomicInteger(0);

        Dispatcher disp = new Dispatcher(Executors.newCachedThreadPool());
        disp.setMaxRequests(1000);
        disp.setMaxRequestsPerHost(1000);

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(Duration.of(2, ChronoUnit.SECONDS))
                .retryOnConnectionFailure(true)
                .connectionPool(new ConnectionPool(400, 5, TimeUnit.MINUTES))
                .dispatcher(disp)
                .build();


        for (ScenarioRequest sr : requestScenario) {
            LOGGER.info(String.format("Sending %s requests in this batch", sr.getBatchSize()));

            for (Request r : sr.getRequests()) {
                client.newCall(r).enqueue(new Callback() {
                    @Override
                    public void onFailure(@NotNull Call call, @NotNull IOException e) {
                        LOGGER.error("Found exception during send: " + e.getMessage());
                        numberOfFailed.incrementAndGet();
                    }

                    @Override
                    public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                        LOGGER.info("Sent was successful");
                        if (response.isSuccessful()) {
                            LOGGER.debug("Request was sent successfully");
                            numberOfSucc.incrementAndGet();
                        } else {
                            LOGGER.error("Received error status code: " + response);
                        }

                    }
                });
            }
            if (sr.getWait() != null) {
                sr.getWait().run();
            }
            LOGGER.info(String.format("Executor sent %d successful requests and %d failed requests.", numberOfSucc.get(), numberOfFailed.get()));
        }
    }

    public List<ScenarioRequest> getRequestScenario() {
        return requestScenario;
    }
}
