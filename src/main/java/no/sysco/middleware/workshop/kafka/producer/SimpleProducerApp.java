package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Simple Kafka Producer application. It creates a single record to be send to a Kafka Cluster.
 *
 * It does not require a created topic, as it will create a topic automatically (if enabled)
 */
public class SimpleProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private SimpleProducerApp() {
    // Setting Producer configurations
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // Create Producer
    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  /**
   * Send a producer record
   */
  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "hello-world!");
    kafkaProducer.send(record);
  }

  /**
   * Run Producer application
   */
  public static void main(String[] args) throws IOException {
    final SimpleProducerApp simpleProducerApp = new SimpleProducerApp();
    simpleProducerApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
