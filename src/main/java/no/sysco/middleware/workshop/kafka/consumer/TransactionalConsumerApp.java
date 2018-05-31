package no.sysco.middleware.workshop.kafka.consumer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

import static java.lang.System.out;

public class TransactionalConsumerApp implements Runnable {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private TransactionalConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "committable-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    consumerConfigs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
    //DEFAULT
    // consumerConfigs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.name());

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    kafkaConsumer.subscribe(Arrays.asList("simple-topic-1", "simple-topic-2"));

    while (true) {
      final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        String value = consumerRecord.value();
        out.println("Record value: " + value);

        kafkaConsumer.commitSync();
      }
    }
  }

  public static void main(String[] args) {
    TransactionalConsumerApp simpleConsumerApp = new TransactionalConsumerApp();
    simpleConsumerApp.run();
  }
}
