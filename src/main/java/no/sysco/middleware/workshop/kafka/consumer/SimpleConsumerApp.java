package no.sysco.middleware.workshop.kafka.consumer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class SimpleConsumerApp implements Runnable {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private SimpleConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    //AUTO ASSIGNMENT
    kafkaConsumer.subscribe(Collections.singletonList("simple-topic"));
    //MANUAL ASSIGNMENT
    // kafkaConsumer.assign(Collections.singletonList(new TopicPartition("simple-topic", 0)));

    while (true) {
      final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        String value = consumerRecord.value();
        out.println("Record value: " + value);
      }
    }
  }

  public static void main(String[] args) {
    SimpleConsumerApp simpleConsumerApp = new SimpleConsumerApp();
    simpleConsumerApp.run();
  }
}
