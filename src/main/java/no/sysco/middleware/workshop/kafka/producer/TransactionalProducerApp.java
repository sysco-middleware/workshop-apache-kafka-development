package no.sysco.middleware.workshop.kafka.producer;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a Producer that has transaction enabled and is able to send several records as part of
 * the same transaction. This is achieved initializing, then starting a transaction, send records
 * and committing/rollback transaction.
 */
public class TransactionalProducerApp {

  private final KafkaProducer<String, String> kafkaProducer;

  private TransactionalProducerApp() {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "simple-transactional-app");

    kafkaProducer = new KafkaProducer<>(producerConfigs);
  }

  private void sendTxRecords() {
    try {
      kafkaProducer.beginTransaction();
      final ProducerRecord<String, String> record1 =
          new ProducerRecord<>("simple-topic-1", "record");
      kafkaProducer.send(record1);
      final ProducerRecord<String, String> record2 =
          new ProducerRecord<>("simple-topic-2", "record");
      kafkaProducer.send(record2);
      kafkaProducer.commitTransaction();
    } catch (Throwable t) {
      t.printStackTrace();
      kafkaProducer.abortTransaction();
    }
  }

  public static void main(String[] args) throws IOException {
    final TransactionalProducerApp transactionalProducerApp = new TransactionalProducerApp();
    transactionalProducerApp.kafkaProducer.initTransactions();

    transactionalProducerApp.sendTxRecords();

    System.out.println("Press ENTER to exit the system");
    System.in.read();
  }
}
