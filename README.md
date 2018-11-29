# Beast

Kafka to BigQuery Sink


### Task List:
 * [x] convert dynamic message -> Map<String,Object> -> Record
 * [x] get List<Dynamic msg> from ConsumerRecords<K,V> (using stencil)
 * [x] consumer to consume messages 


### Laundary List
* Add `errors` to the `Status` object, in case there is partial success for a batch of messages.
* remove artifactory creds
* validation on configuration eg: duplicates in `proto_field_mappings`
* retry on fail
* Implement DLQ

