# Beast

Kafka to BigQuery Sink

### Setup:
Run `cp env/sample.properties env/local.properties` and update the values

### Task List:
* Fix tests - `shouldPushMessagesToBq`, `shouldCommitOffsetsInSequenceWhenAcknowledgedRandom` for CI
* Resiliency
* No data loss
* Kill process, when something goes wrong
* Handle repeated, Duration, bytes, map 
* Retry mechanism
* DLQ
* Add timestamps in logs
* Monitoring - kafka lag, pod level etc
* Add total consumer threads & worker threads in metrics
* Until not in production use `latest` tag for docker images
* Helm chart for ease of deployment

### Laundry List
* [] remove artifactory creds
* [] validation on configuration eg: duplicates in `proto_field_mappings`
* [] Clean up `build.gradle` and upgrade it
* [] Copy jacaco and checkstyle reports to test artifacts
* [] Add tests for stats.java
* [] Change the existing test & build stages, to use `compress` command from systems script 
* [] Test for synchronised threads for kafka consumer
* [] Interface for kafka consumer (wrap threads in synchronised block)
* [] Improve CI build time
* [] Explore `awaitility` for tests

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
* [] Kafka Consumer Offset overflow?
* [] Custom Mappers
* [] Functions & Future
* [] Queue Implementations and performance

## Commands:
- query total records
```
bq query --nouse_legacy_sql 'SELECT count(*) FROM `bq-project.bqsinktest.bq_table` LIMIT 10'
```
- update bq schema from local schema json file
```
bq update --format=prettyjson bq-project:bqsinktest.bq_table  booking.schema
```
-  dump the schema of table to fileA
```
bq show --schema --format=prettyjson bq-project:bqsinktest.bq_table > booking.schem
```
