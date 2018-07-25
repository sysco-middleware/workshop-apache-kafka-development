package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Producer Application that instantiate a Producer with different Acknowledge configurations.
 *
 * It re-use the topic created by SimpleProducer app, or creates a topic (if enabled)
 */
public class ProducerWithAckApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private ProducerWithAckApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    // Acknowledge from all replicas (at least the ones defined `min.insync.replicas`)
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    // Acknowledge from none of the replicas
    // producerConfigs.put(ProducerConfig.ACKS_CONFIG, "0");
    // Acknowledge from leader replica
    // producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1");

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
  }

  /**
   * Run Producer application
   */
  public static void main(String[] args) throws IOException {
    final ProducerWithAckApp producerWithAckApp = new ProducerWithAckApp();

    producerWithAckApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
