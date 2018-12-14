package com.gojek.beast.launch;

import com.gojek.beast.Clock;
import com.gojek.beast.commiter.Committer;
import com.gojek.beast.commiter.OffsetCommitter;
import com.gojek.beast.commiter.OffsetState;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.KafkaConfig;
import com.gojek.beast.config.WorkerConfig;
import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.models.Records;
import com.gojek.beast.parser.ProtoParser;
import com.gojek.beast.sink.MultiSink;
import com.gojek.beast.sink.QueueSink;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.worker.BqQueueWorker;
import com.gojek.beast.worker.ConsumerWorker;
import com.gojek.beast.worker.Worker;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

@Slf4j
public class Main {
    public static void main(String[] args) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getenv());
        Map<String, Object> consumerConfig = new KafkaConfig(appConfig.getKafkaConfigPrefix()).get();
        ColumnMapping columnMapping = appConfig.getProtoColumnMapping();

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Pattern.compile(appConfig.getKafkaTopic()), new NoOpConsumerRebalanceListener());

        //BigQuery
        Sink bqSink = buildBqSink(appConfig);

        BlockingQueue<Records> readQueue = new LinkedBlockingQueue<>(appConfig.getReadQueueCapacity());

        BlockingQueue<Records> committerQueue = new LinkedBlockingQueue<>(appConfig.getCommitQueueCapacity());
        QueueSink queueSink = new QueueSink(readQueue);
        Set<Map<TopicPartition, OffsetAndMetadata>> partitionsAck = Collections.synchronizedSet(new CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>>());
        OffsetCommitter committer = new OffsetCommitter(committerQueue, partitionsAck, kafkaConsumer, new OffsetState(appConfig.getOffsetAckTimeoutMs()));
        MultiSink multiSink = new MultiSink(Arrays.asList(queueSink, committer));


        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(appConfig.getStencilUrl(), new HashMap<>()), appConfig.getProtoSchema());
        ConsumerRecordConverter parser = new ConsumerRecordConverter(new RowMapper(columnMapping), protoParser, new Clock());
        MessageConsumer messageConsumer = new MessageConsumer(kafkaConsumer, multiSink, parser, appConfig.getConsumerPollTimeoutMs());


        new Thread(committer).start();

        List<Worker> workers = spinBqWorkers(appConfig, readQueue, bqSink, committer);
        workers.add(committer);
        addShutDownHooks(workers);

        // This is blocking one !!!
        spinConsumers(messageConsumer);
    }

    private static Sink buildBqSink(AppConfig appConfig) {
        BigQuery bq = getBigQueryInstance(appConfig);
        return new BqSink(bq, TableId.of(appConfig.getDataset(), appConfig.getTable()));
    }

    private static BigQuery getBigQueryInstance(AppConfig appConfig) {
        GoogleCredentials credentials = null;
        File credentialsPath = new File(appConfig.getGoogleCredentials());
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build().getService();
    }

    private static void addShutDownHooks(List<Worker> workers) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            workers.forEach(Worker::stop);
        }));
    }

    private static List<Worker> spinConsumers(MessageConsumer messageConsumer) {
        ConsumerWorker worker = new ConsumerWorker(messageConsumer);
        worker.run();
        return Arrays.asList(worker);
    }

    private static List<Worker> spinBqWorkers(AppConfig appConfig, BlockingQueue<Records> queue, Sink bqSink, Committer committer) {
        Integer bqWorkerPoolSize = appConfig.getBqWorkerPoolSize();
        List<Worker> threads = new ArrayList<>(bqWorkerPoolSize);
        for (int i = 0; i < bqWorkerPoolSize; i++) {
            Worker bqQueueWorker = new BqQueueWorker(queue, bqSink, new WorkerConfig(appConfig.getBqWorkerPollTimeoutMs()), committer);
            Thread bqWorkerThread = new Thread(bqQueueWorker);
            bqWorkerThread.start();
            threads.add(bqQueueWorker);
        }
        return threads;
    }
}
