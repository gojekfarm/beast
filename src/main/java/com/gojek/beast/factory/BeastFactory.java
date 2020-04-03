package com.gojek.beast.factory;

import com.gojek.beast.Clock;
import com.gojek.beast.backoff.BackOff;
import com.gojek.beast.backoff.ExponentialBackOffProvider;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.BQConfig;
import com.gojek.beast.config.BackOffConfig;
import com.gojek.beast.config.StencilConfig;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.config.KafkaConfig;
import com.gojek.beast.protomapping.Converter;
import com.gojek.beast.protomapping.Parser;
import com.gojek.beast.protomapping.ProtoUpdateListener;
import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.commiter.OffsetAcknowledger;
import com.gojek.beast.commiter.OffsetState;
import com.gojek.beast.consumer.KafkaConsumer;
import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.consumer.RebalanceListener;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.MultiSink;
import com.gojek.beast.sink.QueueSink;
import com.gojek.beast.sink.RetrySink;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.BQRowWithInsertId;
import com.gojek.beast.sink.bq.BQRowWithoutId;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.sink.bq.handler.BQErrorHandler;
import com.gojek.beast.sink.bq.handler.BQResponseParser;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.gojek.beast.sink.bq.handler.ErrorWriter;
import com.gojek.beast.sink.bq.handler.DefaultLogWriter;
import com.gojek.beast.sink.bq.handler.gcs.GCSErrorWriter;
import com.gojek.beast.sink.bq.handler.impl.OOBErrorHandler;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.worker.BqQueueWorker;
import com.gojek.beast.worker.ConsumerWorker;
import com.gojek.beast.worker.OffsetCommitWorker;
import com.gojek.beast.worker.Worker;
import com.gojek.beast.worker.WorkerState;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

@Slf4j
public class BeastFactory {
    private final BackOffConfig backOffConfig;
    private final WorkerState workerState;
    private final ProtoUpdateListener protoUpdateListener;
    private AppConfig appConfig;
    private KafkaConsumer kafkaConsumer;
    private OffsetCommitWorker committer;
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionsAck;
    private BlockingQueue<Records> readQueue;
    private MultiSink multiSink;
    private MessageConsumer messageConsumer;
    private LinkedBlockingQueue<Records> commitQueue;
    private BQConfig bqConfig;

    public BeastFactory(AppConfig appConfig, BackOffConfig backOffConfig, StencilConfig stencilConfig, BQConfig bqConfig, ProtoMappingConfig protoMappingConfig, WorkerState workerState) throws IOException {
        this.appConfig = appConfig;
        this.bqConfig = bqConfig;
        this.partitionsAck = Collections.synchronizedSet(new CopyOnWriteArraySet<>());
        this.readQueue = new LinkedBlockingQueue<>(appConfig.getReadQueueCapacity());
        this.commitQueue = new LinkedBlockingQueue<>(appConfig.getCommitQueueCapacity());
        this.backOffConfig = backOffConfig;
        this.workerState = workerState;
        this.protoUpdateListener = new ProtoUpdateListener(protoMappingConfig, stencilConfig, bqConfig, new Converter(), new Parser(), getBigQueryInstance());
    }

    public List<Worker> createBqWorkers() {
        Integer bqWorkerPoolSize = appConfig.getBqWorkerPoolSize();
        List<Worker> threads = new ArrayList<>(bqWorkerPoolSize);
        Acknowledger acknowledger = createAcknowledger();
        log.info("BQ Row InsertId is: {}", (bqConfig.isBQRowInsertIdEnabled()) ? "Enabled" : "Disabled");
        for (int i = 0; i < bqWorkerPoolSize; i++) {
            Worker bqQueueWorker = new BqQueueWorker("bq-worker-" + i, createBigQuerySink(), new QueueConfig(appConfig.getBqWorkerPollTimeoutMs()), acknowledger, readQueue, workerState);
            threads.add(bqQueueWorker);
        }
        return threads;
    }

    public Sink createBigQuerySink() {
        BigQuery bq = getBigQueryInstance();
        BQResponseParser responseParser = new BQResponseParser();
        BQErrorHandler bqErrorHandler = createOOBErrorHandler();
        BQRow recordInserter = new BQRowWithInsertId();
        if (!bqConfig.isBQRowInsertIdEnabled()) {
            recordInserter = new BQRowWithoutId();
        }
        Sink bqSink = new BqSink(bq, TableId.of(bqConfig.getDataset(), bqConfig.getTable()), responseParser, bqErrorHandler, recordInserter);
        return new RetrySink(bqSink, new ExponentialBackOffProvider(backOffConfig.getExponentialBackoffInitialTimeInMs(), backOffConfig.getExponentialBackoffMaximumTimeInMs(), backOffConfig.getExponentialBackoffRate(), new BackOff()), appConfig.getMaxPushAttempts());
    }

    public BQErrorHandler createOOBErrorHandler() {
        final Storage gcsStore = getGCStorageInstance();
        ErrorWriter errorWriter = new DefaultLogWriter();
        if (appConfig.isGCSErrorSinkEnabled()) {
            final String bucketName = appConfig.getGcsBucket();
            final String basePathPrefix = appConfig.getGcsPathPrefix();
            errorWriter = new GCSErrorWriter(gcsStore, bucketName, basePathPrefix);
        }
        return new OOBErrorHandler(errorWriter);
    }

    private BigQuery getBigQueryInstance() {
        final TransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions().toBuilder()
                .setConnectTimeout(Integer.parseInt(bqConfig.getBqClientConnectTimeout()))
                .setReadTimeout(Integer.parseInt(bqConfig.getBqClientReadTimeout()))
                .build();
        return BigQueryOptions.newBuilder()
                .setTransportOptions(transportOptions)
                .setCredentials(getGoogleCredentials())
                .setProjectId(bqConfig.getGCPProject())
                .build().getService();
    }

    private Storage getGCStorageInstance() {
        return StorageOptions.newBuilder()
                .setCredentials(getGoogleCredentials())
                .setProjectId(appConfig.getGcsWriterProject())
                .build().getService();
    }

    private GoogleCredentials getGoogleCredentials() {
        GoogleCredentials credentials = null;
        File credentialsPath = new File(bqConfig.getGoogleCredentials());
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return credentials;
    }

    public OffsetCommitWorker createOffsetCommitter() {
        if (committer != null) {
            return committer;
        }
        OffsetState offsetState = new OffsetState(partitionsAck, appConfig.getOffsetAckTimeoutMs(), appConfig.getOffsetCommitTime());
        committer = new OffsetCommitWorker("committer", new QueueConfig(appConfig.getBqWorkerPollTimeoutMs(), "commit"), createKafkaConsumer(), offsetState, commitQueue, workerState, new Clock());
        return committer;
    }

    private MultiSink createMultiSink() {
        if (multiSink != null) {
            return multiSink;
        }
        QueueSink readQueueSink = new QueueSink(readQueue, new QueueConfig(appConfig.getBqWorkerPollTimeoutMs(), "read"));
        QueueSink committerQueueSink = new QueueSink(commitQueue, new QueueConfig(appConfig.getBqWorkerPollTimeoutMs(), "commit"));
        multiSink = new MultiSink(Arrays.asList(readQueueSink, committerQueueSink));
        return multiSink;
    }

    private MessageConsumer createMessageConsumer() {
        if (messageConsumer != null) return messageConsumer;
        messageConsumer = new MessageConsumer(createKafkaConsumer(), createMultiSink(), protoUpdateListener, appConfig.getConsumerPollTimeoutMs());
        return messageConsumer;
    }

    private Acknowledger createAcknowledger() {
        return new OffsetAcknowledger(partitionsAck);
    }

    private KafkaConsumer createKafkaConsumer() {
        if (kafkaConsumer != null) {
            return kafkaConsumer;
        }
        Map<String, Object> consumerConfig = new KafkaConfig(appConfig.getKafkaConfigPrefix()).get(appConfig);
        org.apache.kafka.clients.consumer.KafkaConsumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(consumerConfig);
        consumer.subscribe(Pattern.compile(appConfig.getKafkaTopic()), new RebalanceListener());
        kafkaConsumer = new KafkaConsumer(consumer);
        return kafkaConsumer;
    }

    public Worker createConsumerWorker() {
        return new ConsumerWorker("consumer", createMessageConsumer(), workerState);
    }

    public void close() throws IOException {
        log.debug("Closing beast factory");
        readQueue.clear();
        workerState.closeWorker();
        protoUpdateListener.close();
        Stats.stop();
    }
}
