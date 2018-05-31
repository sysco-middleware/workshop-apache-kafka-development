package no.sysco.middleware.workshop.kafka.consumer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class CommittableConsumerApp implements Runnable {

  private final KafkaConsumer<String, String> kafkaConsumer;

  private CommittableConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "committable-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    //AT-MOST-ONCE BY FREQUENCE
    consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Duration.ofSeconds(1).toMillis());

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    kafkaConsumer.subscribe(Collections.singletonList("simple-topic"));

    while (true) {
      final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        String value = consumerRecord.value();
        out.println("Record value: " + value);

        //AT-LEAST-ONCE BLOCKING
        // kafkaConsumer.commitSync();

        //AT-LEAST-ONCE NON-BLOCKING
        //kafkaConsumer.commitAsync();
      }

      //AT-MOST-ONCE BY BATCH, BLOCKING
      // kafkaConsumer.commitSync();

      //AT-MOST-ONCE BY BATCH, NON-BLOCKING
      // kafkaConsumer.commitAsync();
    }
  }

  public static void main(String[] args) {
    CommittableConsumerApp simpleConsumerApp = new CommittableConsumerApp();
    simpleConsumerApp.run();
  }
}
