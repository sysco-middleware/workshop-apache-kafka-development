package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerWithBatchingApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private ProducerWithBatchingApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, 100 * 1024); //buffer 100KB
    producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG, Duration.ofSeconds(10).toMillis()); //wait for buffer to be full

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record, (metadata, exception) -> {
      System.out.println("Ack received");
    });
  }

  public static void main(String[] args) throws IOException {
    final ProducerWithBatchingApp producerWithAckApp = new ProducerWithBatchingApp();
    IntStream.range(0, 1000).forEach(ignored -> producerWithAckApp.sendRecord());

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
