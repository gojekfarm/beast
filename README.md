# Beast

Kafka to BigQuery Sink

Note: Until not in production use `latest` tag for docker images. Also, we don't support Struct protobuf fields.

### Setup:
* For Terminal - Run `cp env/sample.properties env/local.properties` and update the values
* For IntelliJ - Install the `envfile` plugin, and create envfile with `cp env/sample.properties env/local.env`. Then source `local.env` in envfile settings.

### Task List:
* Fix tests - `shouldPushMessagesToBq`, `shouldCommitOffsetsInSequenceWhenAcknowledgedRandom` for CI
* Resiliency
* No data loss
* Kill process, when something goes wrong
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
* Use java 10/11
* Find package like factorybot, and make factories
* Refactor KafkaConsumerUtil
* Add Draft for development
* Add option to disable sending stats

### Reading List
* Kafka Consumer Offset overflow?
* Custom Mappers
* Functions & Future
* Queue Implementations and performance

<<<<<<< HEAD
=======
## Sample schema/configuration:
Proto column mappings & BigQuery table schemas are available in `schema` directory

## Commands:
- create new table
```
bq mk --table <project_name>:<dataset_name>.<table_name> <path_to_schema_file>
```
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
bq show --schema --format=prettyjson bq-project:bqsinktest.bq_table > booking.schema
```
