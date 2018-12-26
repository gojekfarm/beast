# Beast

Kafka to BigQuery Sink

Note: Until not in production use `latest` tag for docker images

### Setup:
Run `cp env/sample.properties env/local.properties` and update the values

### Task List:
* Fix tests - `shouldPushMessagesToBq`, `shouldCommitOffsetsInSequenceWhenAcknowledgedRandom` for CI
* Resiliency
* No data loss
* Kill process, when something goes wrong
* Handle `map` proto fields
* Add total consumer threads & worker threads in metrics
* Add integration test with BQ (separate stage in pipeline)
* BqSinkWorker - bqsink.push() could be made as async with commit in callback

### Laundry List
* Copy jacaco and checkstyle reports to test artifacts
* Add tests for stats.java
* Test for synchronised threads for kafka consumer
* Explore `awaitility` for tests
* Retry mechanism
* DLQ

### Enhancements
* Use java 11
* Find package like factorybot, and make factories
* Refactor KafkaConsumerUtil
* Add Draft for development
* Add option to disable sending stats

### Reading List
* Kafka Consumer Offset overflow?
* Custom Mappers
* Functions & Future
* Queue Implementations and performance

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
