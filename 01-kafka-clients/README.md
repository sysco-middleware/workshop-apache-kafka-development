# Kafka Clients

Apache Kafka clients includes: 

* Kafka Producer API: Produce and Send Records to a Kafka Cluster.
* Kafka Consumer API: Poll and Receive Records from Kafka Cluster.
* Kafka Admin API: Manage Kafka Components (e.g. topics, consumer groups, metadata)

On top of these, Kafka Connect (lesson 4) and Kafka Streams (lesson 5) are implemented, 
we will check those later.

To have an understanding of these basic APIs, we will review each one. 

## Getting Started

To start playing with Clients, we will first define a Maven dependency to import libraries:

```
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>1.1.0</version>
    </dependency>
```

This will include: Admin, Producer and Consumer APIs.

## Kafka Producer API

Applications can use this API to send `Record`s to an Apache Kafka cluster.

Here is a simple application that just send 'hello-world' Records to Kafka:

> Source: `src/main/java/no/sysco/middleware/kafka/producer/SimpleProducerApp.java`

```java
public class SimpleProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private SimpleProducerApp() {
    final Properties producerConfigs = new Properties(); // (1)
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    kafkaProducer = new KafkaProducer<>(producerConfigs); // (2)
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "hello-world!");
    kafkaProducer.send(record); // (3)
  }

  public static void main(String[] args) throws IOException {
    SimpleProducerApp simpleProducerApp = new SimpleProducerApp();
    simpleProducerApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
```

1. Set Producer Configurations, at least the 3 minimun required are: Bootstrap Servers, Key Serializer Class, 
and Value Serializer Class. 
2. Instantiate a `KafkaProducer`. 
3. Send records.

We will go in more detail about each phase in a moment. 

To test it, just run this Application. It will wait for you to press ENTER to finish. 

To validate that a record is published, execute `kafka-console-consumer` tool:

```
$ kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
hello-world!
^CProcessed a total of 1 messages
```

### Basic Configuration Properties

#### Bootstrap Servers

List of Server that will be used by the Producer to ask for all the `Brokers` available in a `Cluster`. 

For instance, if you have a Cluster with 3 nodes, and your bootstrap only have 1, once `Producer` stablish
connection with the Cluster, it will receive a list of all the Available nodes.

#### Key and Value Serializer Class

As we mentioned before, a `Record` is an object with Key, Value and additional metadata (e.g. timestamp, headers, etc.)

Then, when we instantiate a Producer, we need to define how Key and Value objects will become a byte array.

These Serializers have to be aligned with `KafkaProducer<K, V>` generic types (e.g. `K`, `V`). If we define a `StringSerializer`,
for `K` and `V`, then KafkaProducer should be instantiated: `KafkaProducer<String, String>`. 

This generic types will propagate to `ProducerRecord<K, V>` to create records.

### Important Configuration Properties

Even though the first 3 properties are enough to connect and send records to Kafka, there are additional ones that are even 
more important depending on the level of delivery semantics that a use-case require (e.g. at-least-one, exactly-once)

#### Acknowledge

> Source: `src/main/java/no/sysco/middleware/kafka/producer/ProducerWithAckApp.java`

```java
    producerConfigs.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    // producerConfigs.setProperty(ProducerConfig.ACKS_CONFIG, "0");
    // producerConfigs.setProperty(ProducerConfig.ACKS_CONFIG, "1");
```

Acknowledge property defines the number of acknowledgments a Producer requires to confirm that a record is sent.

This is closely related with the use-case. 

* `ack=0` means that a Producer won't wait for a Cluster to confirm that a message has been received. This can
be useful when loosing a message is _not_ critical, and we prefer lower latency over durability. e.g. User clicks, Sensor data.
* `ack=1` means that a Producer will wait only for a Leader Replica that a message has been received. Loosing a message 
here is less probable but still possible, if broker where leader replica is hosted is lost before message arrive to follower 
replicas (i.e. other brokers). 
* `ack=all` or `ack=-1` means that a Producer will wait for Leader and Follower replicas to confirm that message is received. 
Here you confirm that message is not lost (at least all cluster is lost and not back-up'd). This increase latency as there is 
an additional network round-trip to other brokers.

If `ack=all` and we want to reduce latency without loosing durability guarantees, we can set `min.insync.replicas=<number of replicas>` to 
decrease the minimun number of replicas that have to confirm a message is received. 
For instance, a Cluster with 5 nodes, and `min.insync.replicas=3` will require the majority of replicas to confirm that a 
record has arrived.

#### Batching

Kafka Producer uses an internal mechanism to buffer records and send them as batches. This to increase throughput. 
To manage this feature we have some configuration parameters to buffer data:

```java
    producerConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, 100 * 1024); //buffer 100KB
    producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG, Duration.ofSeconds(10).toMillis()); //wait for buffer to be full
```

* `batch.size` defines up to what size Recors can be hold together to be send as a batch to a Cluster.
* `linger.ms` defines how long to wait for `batch.size` to be full. If not, Producer will send what is available.

There are other cases where batching is not required, and we want to enforce to send current Records in buffer (even if it is
only one). In this cases, we can use `Producer#flush()` operation:

```java
  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
    kafkaProducer.flush();
  }
```

#### "In-flight" Requests per Connection and Retries

By default, Kafka Producer has a property `max.in.flight.requests.per.connection` set to `5`. This means that 
there could up to 5 requests unacknowledged. In a case where 5 requests are sent, and the 4th one fails, Kafka
Producer will retry, based on `retries=0`, potentially sending messages un-ordered.

In cases where we want to keep strict order, we can set `max.in.flight.requests.per.connection=1` and avoid 
unorder scenarios.

```java
    producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    producerConfigs.put(ProducerConfig.RETRIES_CONFIG, 5);
    producerConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
```

#### Exactly-once Semantics and Transactions

Since version `0.11` Kafka add support for Exactly-once Semantics (`EoS`) and Transactions. This involves
a couple of properties:

* `enable.idempotence` means that Kafka will ensure that Records are not duplicated. If `true`, then 
`max.in.flight.requests.per.connection` should be less or equal to `5`, `retries > 0` and `acks=all`
* `transactional.id` is used for transactions that span multiple topic partition. In this scenarios, if multiple
`send(ProducerRecord)` are issued, this execution will ensure that all or none of these are commited.

```java
public class TransactionalProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private TransactionalProducerApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "simple-transactional-app"); //(1)

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecords() {
    try {
      kafkaProducer.beginTransaction(); //(3)
      final ProducerRecord<String, String> record1 =
          new ProducerRecord<>("simple-topic-1", "record");
      kafkaProducer.send(record1);
      final ProducerRecord<String, String> record2 =
          new ProducerRecord<>("simple-topic-2", "record");
      kafkaProducer.send(record2);
      kafkaProducer.commitTransaction(); //(4)
    } catch (Throwable t) {
      t.printStackTrace();
      kafkaProducer.abortTransaction(); //(5)
    }
  }

  public static void main(String[] args) throws IOException {
    final TransactionalProducerApp transactionalProducerApp = new TransactionalProducerApp();
    transactionalProducerApp.kafkaProducer.initTransactions(); //(2)
    transactionalProducerApp.sendRecords();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
```

1. Transactinal Producer is enabled. This requires idempotency.
2. Initialize transactional app before starting transactions. This prepare the broker for transactions from this application.
3. Begin transaction. This could includes many `send()` operations.
4. Commit transaction.
5. If something fails, abort transaction.

## Kafka Consumer API

Applications use this API to `poll` data from Kafka Topics.

> Source: src/main/java/no/sysco/middleware/workshop/kafka/consumer/SimpleConsumerApp.java

```java
public class SimpleConsumerApp implements Runnable {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private SimpleConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs); //(1)
  }

  public void run() {
    kafkaConsumer.subscribe(Collections.singletonList("simple-topic")); //(2)

    while (true) {
      final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE); //(3)

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        String value = consumerRecord.value(); //(4)
        out.println("Record value: " + value);
      }
    }
  }

  public static void main(String[] args) {
    SimpleConsumerApp simpleConsumerApp = new SimpleConsumerApp();
    simpleConsumerApp.run(); //(5)
  }
}
```

This simple applications is (1) instantiating a `KafkaConsumer` class. This consumer has some
basic configuration required: 

* `ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG`: Kafka nodes,
* `COnsumerConfig.GROUP_ID_CONFIG`: Group Id, and
* `ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG` and `ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,` to
deserialize Key and Value.

Group ID is used to scale different instances of a Consumer Group and process data in parallel. 

> The **number of instances of a consumer group** that can be executed **in parallel** are 
**up to the number of Partitions of a Topic**.
> When  you have more than that, instances are idle.

(2) A Consumer application usually *subscribes* to a Set of Topics. This means that the Cluster will decide
which Topic Partitions to poll from. 

There are some less common cases where you *know* which Topic Partition to use. In this cases you can use
the `assign` method.

(3) Polling data from an assignment is done by using `KafkaConsumer#poll(Long)` method. This operations is
**blocking**, that means that your applications will block this thread while the Timeout is not triggered, 
if no data is received. 

Data is received as a batch of `ConsumerRecord` that can be iterated and processed in parallel.

Step (4) is record consumption. 

and finally, we will run a Consumer Group Instance `thread`.

### Commits and Delivery Semantics

### Offset Management

### Move along the log

### Isolation and Transactions


## Kafka Admin API

### Topics


