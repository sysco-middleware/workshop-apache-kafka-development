package no.sysco.middleware.workshop.kafka.streams;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Text Lines Producer, to be consumed by WordCountStreamsApp
 */
public class TextLinesProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private TextLinesProducerApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("text", "hey mundo");
    kafkaProducer.send(record);
  }

  public static void main(String[] args) throws IOException {
    final TextLinesProducerApp simpleProducerApp = new TextLinesProducerApp();
    simpleProducerApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
