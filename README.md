# Beast

Kafka to BigQuery Sink

### Setup:
Run `cp env/sample.properties env/local.properties` and update the values

### Task List:
 * [] Emit stats
 * [] Offset Committer consumer commit currently needs to be wrapped in synchronized
 * [] Test for synchronised threads for kafka consumer
 * [] Interface for kafka consumer (wrap threads in synchronised block)
 * [] Add topic, partition, offset and bq_creation_timestamp to the created BQ table for auditing

### Laundry List
* [] remove artifactory creds
* [] When specifying kafka topic, use regex, rather tha singleton
* [] validation on configuration eg: duplicates in `proto_field_mappings`
* [] retry on fail
* [] Implement DeadLetterQueue
* [] Add logging configuration from env vars
* [] Refactor `BQSink.push()`
* [] Clean up `build.gradle` and upgrade it
* [] Inject BQ creation timestamp in the table

### Enhancements
* [] BqSinkWorker - bqsink.push() could be made as async with commit in callback
* [] Use jib to create docker image
* [] Use java 11
* [] Find package like factorybot, and make factories
* [] Refactor KafkaConsumerUtil
* [] Update all libraries to latest version
* [] Add Draft for development

### Reading List
* [] Custom Mappers
* [] Functions & Future
* [] Queue Implementations and performance
