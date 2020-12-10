package com.gojek.beast.launch;

import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.BackOffConfig;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.config.StencilConfig;
import com.gojek.beast.config.BQConfig;
import com.gojek.beast.factory.BeastFactory;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.worker.Worker;
import com.gojek.beast.worker.WorkerState;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.List;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getenv());
        ProtoMappingConfig protoMappingConfig = ConfigFactory.create(ProtoMappingConfig.class, System.getenv());
        BackOffConfig backOffConfig = ConfigFactory.create(BackOffConfig.class, System.getenv());
        StencilConfig stencilConfig = ConfigFactory.create(StencilConfig.class, System.getenv());
        BQConfig bqConfig = ConfigFactory.create(BQConfig.class, System.getenv());
        WorkerState workerState = new WorkerState();
        Stats statsClient = Stats.client();

        BeastFactory beastFactory = null;
        try {
            beastFactory = new BeastFactory(appConfig, backOffConfig, stencilConfig, bqConfig, protoMappingConfig, workerState);

            Worker consumerThread = beastFactory.createConsumerWorker();
            consumerThread.start();

            List<Worker> workers = beastFactory.createBqWorkers();
            workers.forEach(Thread::start);

            Worker committerThread = beastFactory.createOffsetCommitter();
            committerThread.start();

            addShutDownHooks(workerState);
            consumerThread.join();
            log.debug("Joined on consumer thread");
            committerThread.join();
            log.debug("Joined on committer thread");
            for (Worker worker : workers) {
                worker.join();
                log.debug("Joined on worker {} thread", worker.getName());
            }
            log.debug("Joined on all worker threads");
        } catch (Throwable e) {
            log.error("Exception: {}", e.getMessage());
            statsClient.increment("global.errors,exception=" + e.getClass().getName());
            if (e instanceof InterruptedException) {
                log.error("Exception::KafkaConsumer and committer join failed: {}", e.getMessage());
            }
            throw e;
        } finally {
            if (beastFactory != null) {
                beastFactory.close();
            }
        }
        log.info("Beast process completed");
    }

    private static void addShutDownHooks(WorkerState workerState) {
        Runtime.getRuntime().addShutdownHook(new Thread(workerState::closeWorker));
    }
}
