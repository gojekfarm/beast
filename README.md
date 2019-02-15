# Beast

Kafka to BigQuery Sink

## Architecture

* Consumer - Consumes messages from kafka in batches, and pushes these batches to Read & Commit queues. These queues are blocking queues, i.e, no more messages will be consumed if the queue is full.
* BigQuery Worker - Polls messages from the read queue, and pushes them to BigQuery. If the push operation was successful, BQ worker sends an acknowledgement to the Committer.
* Committer - Committer receives the acknowledgements of successful push to BigQuery from BQ Workers. All these acknowledgements are stored in a set within the committer. Committer polls the commit queue for message batches. If that batch is present in the set, i.e., the batch has been successfully pushed to BQ, then it commits the max offset for that batch, back to Kafka, and pops it from the commit queue & set.

<br><div style="text-align:center;width: 90%; margin:auto;"><img src="docs/images/architecture.png" alt=""></div><br>

Note: Beast does not support Struct protobuf fields.

## Deployment

Beast is primarily deployed on kubernetes. An individual beast deployment is required for each topic in kafka, that needs to be pushed to BigQuery. A single pod consists of:
* one kafka consumer
* several BQ workers
* one committer

For deploying beast on kubernetes, we need:
* Deployment
* ConfigMap
* Secret - containing credentials to BigQuery (can be shared by all deployments)

### Setup:
* For Terminal - Run `cp env/sample.properties env/local.properties` and update the values
* For IntelliJ - Install the `envfile` plugin, and create envfile with `cp env/sample.properties env/local.env`. Then source `local.env` in envfile settings.

### Building & Running

* To build:
`export $(cat ./env/sample.properties | xargs -L1) && gradle clean build`

* To run:
`export $(cat ./env/sample.properties | xargs -L1) && gradle clean runConsumer`

### Task List:
* Resiliency
* Add integration test with BQ (separate stage in pipeline)
* Add tests for Factories
    - verify it shares queue
    - verify it shares workerstate
* Add WorkerTest (ensure other worker stops on singleton state change)
* Fix Ignored Tests
* Push stats: remaining queue size

### Laundry List
* Copy jacaco and checkstyle reports to test artifacts
* Add tests for stats.java
* Test for synchronised threads for kafka consumer

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

## BQ CLI Commands:
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
