# Beast

Kafka to BigQuery Sink

Note: Until not in production use `latest` tag for docker images. Also, we don't support Struct protobuf fields.

### Setup:
* For Terminal - Run `cp env/sample.properties env/local.properties` and update the values
* For IntelliJ - Install the `envfile` plugin, and create envfile with `cp env/sample.properties env/local.env`. Then source `local.env` in envfile settings.

### Building & Running

* To build:
`export $(cat ./env/sample.properties | xargs -L1) && gradle clean build`

* To run:
`export $(cat ./env/sample.properties | xargs -L1) && gradle clean runConsumer`

### Task List:
* Fix tests - `shouldPushMessagesToBq`, `shouldCommitOffsetsInSequenceWhenAcknowledgedRandom` for CI
* Resiliency
* No data loss
* Add integration test with BQ (separate stage in pipeline)
* Rename committer to Acknowledger
* offsetCommiter pull out Worker part
* Remove Sink from OffsetCommitter
* Introduce event library, close -> send event so subscribers can stop
* Factories
* Fix Ignored Tests
* Data loss
    - verified in kafka messages (the lost message is available)
        - by rerunning consumer
        - also checking in log segments in data-logs of the specific partition
    - verified the logs ,which shows that the partition,offset is committed (after BQ push)
    - manually injected data with log-feeder and verified - no data loss (couldn't reproduce issue)
    - setting up kibana for easy access of logs (WIP)
    - ran consumer from earliest
    
    - Yet to do : chaos testing tool
    - Bq Client lib load test

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

## Sample schema/configuration:
Proto column mappings & BigQuery table schemas are available in `schema` directory

## Commands:
- create new table
```
bq mk --table <project_name>:<dataset_name>.<table_name> <path_to_schema_file>
```
- query total records
```
bq query --nouse_legacy_sql 'SELECT count(*) FROM `<project_name>:<dataset_name>.<table_name>` LIMIT 10'
```
- update bq schema from local schema json file
```
bq update --format=prettyjson <project_name>:<dataset_name>.<table_name>  booking.schema
```
-  dump the schema of table to fileA
```
bq show --schema --format=prettyjson <project_name>:<dataset_name>.<table_name> > booking.schema
```
