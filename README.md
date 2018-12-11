# Beast

Kafka to BigQuery Sink

### Setup:
Run `cp env/sample.properties env/local.properties` and update the values

### Task List:
 * [] Add topic, partition, offset and bq_creation_timestamp to the created BQ table for auditing

### Laundry List
* [] remove artifactory creds
* [] validation on configuration eg: duplicates in `proto_field_mappings`
* [] retry on fail
* [] Implement DeadLetterQueue
* [] Clean up `build.gradle` and upgrade it
* [] Copy jacaco and checkstyle reports to test artifacts
* [] Add tests for stats.java
* [] Reduce the test pipeline time
* [] Change the existing test & build stages, to use `compress` command from systems script 
* [] Test for synchronised threads for kafka consumer
* [] Interface for kafka consumer (wrap threads in synchronised block)

### Enhancements
* [] BqSinkWorker - bqsink.push() could be made as async with commit in callback
* [] Use jib to create docker image
* [] Use java 11
* [] Find package like factorybot, and make factories
* [] Refactor KafkaConsumerUtil
* [] Update all libraries to latest version
* [] Add Draft for development
* [] Add option to disable sending stats

### Reading List
* [] Custom Mappers
* [] Functions & Future
* [] Queue Implementations and performance
