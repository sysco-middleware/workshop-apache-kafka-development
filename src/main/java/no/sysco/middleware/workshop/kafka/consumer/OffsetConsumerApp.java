package no.sysco.middleware.workshop.kafka.consumer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class OffsetConsumerApp implements Runnable {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private OffsetConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "committable-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //WHERE TO START
    // consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST);
    consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST);

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    kafkaConsumer.subscribe(Collections.singletonList("simple-topic"));

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
    OffsetConsumerApp simpleConsumerApp = new OffsetConsumerApp();
    simpleConsumerApp.run();
  }
}
