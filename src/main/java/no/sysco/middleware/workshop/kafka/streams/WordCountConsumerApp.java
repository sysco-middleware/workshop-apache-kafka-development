package no.sysco.middleware.workshop.kafka.streams;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class WordCountConsumerApp implements Runnable {

  private final KafkaConsumer<String, Long> kafkaConsumer;

  private WordCountConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "word-count-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    kafkaConsumer.subscribe(Collections.singletonList("word-count"));

    while (true) {
      final ConsumerRecords<String, Long> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
        String key = consumerRecord.key();
        Long value = consumerRecord.value();
        out.println("Record key: " + key + " value: " + value);
      }
    }
  }

  public static void main(String[] args) {
    WordCountConsumerApp simpleConsumerApp = new WordCountConsumerApp();
    simpleConsumerApp.run();
  }
}
