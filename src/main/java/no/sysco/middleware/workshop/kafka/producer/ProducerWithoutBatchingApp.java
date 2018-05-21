package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class ProducerWithoutBatchingApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private ProducerWithoutBatchingApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
    kafkaProducer.flush();
  }

  public static void main(String[] args) throws IOException {
    final ProducerWithoutBatchingApp producerWithAckApp = new ProducerWithoutBatchingApp();
    producerWithAckApp.sendRecord();
    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
