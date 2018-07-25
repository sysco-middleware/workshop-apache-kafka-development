package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a Producer application that will only keep 1 in-flight connection un-acknowledge and block
 * the Producer until receiving ack.
 * Only one record send every time, ensure strict order, as retrying will not change sequence of
 * requests, but performance could be affected.
 */
public class StrictOrderProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private StrictOrderProducerApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); //only 1 request per connection
    producerConfigs.put(ProducerConfig.RETRIES_CONFIG, 5);
    producerConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("simple-topic", "record");
    kafkaProducer.send(record);
  }

  public static void main(String[] args) throws IOException {
    final StrictOrderProducerApp producerWithAckApp = new StrictOrderProducerApp();
    producerWithAckApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
