# Beast

Kafka to BigQuery Sink

### Setup:
Run `cp env/sample.properties env/local.properties` and update the values

### Task List:
 * [] Emit stats
 * [] Committer
    * [] get MaxOffset for batch for each partition in Record
    * [] Have a global set, to which BQ worker threads will push acknowledgements
    * [] A thread to commit offsets by using both - CommitQueue & Global set
    * [] Have a `SinkPusher` which will push messages to QueueSink & CommitSink
    
 
 
### Laundary List
* Add `errors` to the `Status` object, in case there is partial success for a batch of messages.
* remove artifactory creds
* When specifying kafka topic, use regex, rather tha singleton
* validation on configuration eg: duplicates in `proto_field_mappings`
* retry on fail
* Implement DeadLetterQueue
* Add logging configuration from env vars
* Refactor `BQSink.push()`
* Clean up `build.gradle` and upgrade it
* Inject BQ creation timestamp in the table

### Enhancements
* BqSinkWorker - bqsink.push() could be made as async with commit in callback
* Use jib to create docker image
* Use java 11
* Find package like factorybot, and make factories
* Refactor KafkaConsumerUtil
* Update all libraries to latest version
* Add Draft for development

### Reading List
* Custom Mappers
* Functions & Future
* Queue Implementations and performance
