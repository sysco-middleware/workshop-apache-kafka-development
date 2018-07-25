package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a Producer to send messages to Kafka one by one, without Batching, or holding messages
 * in memory.
 *
 * This is not recommended if you are trying to achieve high throughput, as it will send each
 * message individually.
 *
 * It will reuse the same topic, or create it.
 */
public class ProducerOneByOneApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private ProducerOneByOneApp() {
    // Setting Producer configs
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
    kafkaProducer.flush();
  }

  /**
   * Run Producer application
   */
  public static void main(String[] args) throws IOException {
    final ProducerOneByOneApp producerWithAckApp = new ProducerOneByOneApp();
    producerWithAckApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
