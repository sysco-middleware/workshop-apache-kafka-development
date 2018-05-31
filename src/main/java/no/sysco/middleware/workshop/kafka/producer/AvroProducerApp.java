package no.sysco.middleware.workshop.kafka.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import no.sysco.middleware.workshop.kafka.CommonProperties;
import no.sysco.middleware.workshop.kafka.avro.Record1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Hello world!
 */
public class AvroProducerApp {

  private final KafkaProducer<Long, Record1> kafkaProducer;

  private AvroProducerApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
    producerConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CommonProperties.SCHEMA_REGISTRY_URL);

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendRecord() {
    final Record1 record1 =
        Record1.newBuilder()
            .setId(1L)
            .setOptionalElement(null)
            .build();

    final ProducerRecord<Long, Record1> record =
        new ProducerRecord<>("avro-topic", record1.getId(), record1);
    kafkaProducer.send(record);
  }

  public static void main(String[] args) throws IOException {
    final AvroProducerApp producerApp = new AvroProducerApp();
    producerApp.sendRecord();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
