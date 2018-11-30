# Beast

Kafka to BigQuery Sink


### Task List:
 * [x] convert dynamic message -> Map<String,Object> -> Record
 * [x] get List<Dynamic msg> from ConsumerRecords<K,V> (using stencil)
 * [x] consumer to consume messages 

### Laundary List
* Add `errors` to the `Status` object, in case there is partial success for a batch of messages.
* remove artifactory creds
* When specifying kafka topic, use regex, rather tha singleton
* validation on configuration eg: duplicates in `proto_field_mappings`
* retry on fail
* Implement DLQ
* Add logging configuration from env vars

### Enhancements
* Use java 10
* Use jib to create docker image
* Find package like factorybot, and make factories
* Refactor KafkaConsumerUtil
* Show code coverage in CLI on test run
* Update all libraries to latest version
* Add Draft for development

### Reading List
* Custom Mappers
* Functions & Future
* Queue Implementations and performance
