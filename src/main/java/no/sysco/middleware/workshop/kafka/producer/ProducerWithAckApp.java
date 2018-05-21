package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class ProducerWithAckApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private ProducerWithAckApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    // producerConfigs.put(ProducerConfig.ACKS_CONFIG, "0");
    // producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1");

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
  }

  public static void main(String[] args) throws IOException {
    final ProducerWithAckApp producerWithAckApp = new ProducerWithAckApp();
    producerWithAckApp.sendRecord();
    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}