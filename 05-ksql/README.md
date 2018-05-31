# KSQL

## Data Discovery

### Topics

List topics:

```sql
ksql> show topics;

 Kafka Topic                                                            | Registered | Partitions | Partition Replicas | Consumers | Consumer Groups 
-----------------------------------------------------------------------------------------------------------------------------------------------------
 __confluent.support.metrics                                            | false      | 1          | 1                  | 0         | 0               
 __consumer_offsets--formatter                                          | false      | 1          | 1                  | 0         | 0               
 _confluent-ksql-default__command_topic                                 | true       | 1          | 1                  | 0         | 0               
 _schemas                                                               | false      | 1          | 1                  | 0         | 0               
 avro-topic                                                             | false      | 1          | 1                  | 0         | 0               
 connect-configs                                                        | false      | 1          | 1                  | 0         | 0               
 connect-offsets                                                        | false      | 25         | 1                  | 0         | 0               
 connect-statuses                                                       | false      | 5          | 1                  | 0         | 0               
 dwh-schemas-avro-number_games-wager_record-v1                          | false      | 3          | 1                  | 0         | 0               
 simple-topic                                                           | false      | 1          | 1                  | 0         | 0               
 simple-topic-1                                                         | false      | 1          | 1                  | 0         | 0               
 simple-topic-2                                                         | false      | 1          | 1                  | 0         | 0               
 text                                                                   | false      | 1          | 1                  | 0         | 0               
 topic-1                                                                | false      | 3          | 1                  | 0         | 0               
 tweets                                                                 | false      | 1          | 1                  | 1         | 1               
 word-count                                                             | false      | 1          | 1                  | 0         | 0               
 word-count-app-v1-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog   | false      | 1          | 1                  | 0         | 0               
 word-count-app-v1-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition | false      | 1          | 1                  | 0         | 0               
 word-count-app-v1-KSTREAM-AGGREGATE-STATE-STORE-0000000005-changelog   | false      | 1          | 1                  | 0         | 0               
 word-count-app-v1-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition | false      | 1          | 1                  | 0         | 0               
-----------------------------------------------------------------------------------------------------------------------------------------------------

```

Print topics content:

```sql
ksql> print 'tweets';
Format:AVRO
...
```

### Select Queries

TODO

## Streams and Tables

TODO