package io.debezium.perf.load.scenarios.builder;

import io.debezium.perf.load.scenarios.ScenarioRequest;
import io.debezium.perf.load.scenarios.ScenarioRequestExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;

public class ConstantScenarioBuilder implements ScenarioBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConstantScenarioBuilder.class);
    private final int rounds;

    private final int rate;

    public ConstantScenarioBuilder(int rounds, int rate) {
        this.rounds = rounds;
        this.rate = rate;
    }

    @Override
    public ScenarioRequestExecutor prepareScenario(List<HttpRequest> requestList) {

        int requestCounter = 0;
        int requestCnt = requestList.size();
        int roundLevel = requestCnt / rounds;

        List<ScenarioRequest> result = new ArrayList<>();

        while (requestCounter < requestCnt) {
            List<HttpRequest> batchList = new ArrayList<>();
            for (int i = 0; i < roundLevel && requestCounter < requestList.size(); i++) {
                batchList.add(requestList.get(i));
                requestCounter++;
            }
            result.add(new ScenarioRequest(batchList, () -> {
                try {
                    LOGGER.debug("Waiting " + rate + " until next request");
                    Thread.sleep(rate);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        return new ScenarioRequestExecutor(result);
    }
}
