package no.sysco.middleware.workshop.kafka.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import no.sysco.middleware.workshop.kafka.CommonProperties;
import no.sysco.middleware.workshop.kafka.avro.Record1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class AvroConsumerApp implements Runnable {

  private final KafkaConsumer<Long, Record1> kafkaConsumer;

  private AvroConsumerApp() {
    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-simple-consumer-v1");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);
    consumerConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CommonProperties.SCHEMA_REGISTRY_URL);

    kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
  }

  public void run() {
    kafkaConsumer.subscribe(Collections.singletonList("avro-topic"));

    while (true) {
      final ConsumerRecords<Long, Record1> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<Long, Record1> consumerRecord : consumerRecords) {
        String key = consumerRecord.key().toString();
        String record = consumerRecord.value().toString();
        out.println("Record key: " + key + " - Record value: " + record);
      }
    }
  }

  public static void main(String[] args) {
    AvroConsumerApp simpleConsumerApp = new AvroConsumerApp();
    simpleConsumerApp.run();
  }
}
