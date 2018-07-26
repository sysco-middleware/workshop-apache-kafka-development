package no.sysco.middleware.workshop.kafka.streams;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import no.sysco.middleware.workshop.kafka.admin.TopicsApp;
import no.sysco.middleware.workshop.kafka.streams.util.KafkaStreamsTopologyGraphvizPrinter;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.System.out;

/**
 * Simplest streaming application to count words from text paragraphs
 */
public class WordCountStreamsApp {
  private final KafkaStreams kafkaStreams;

  private WordCountStreamsApp() {
    final Properties streamsConfigs = new Properties();
    streamsConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    streamsConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app-v1");
    streamsConfigs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    streamsConfigs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    streamsConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
    streamsConfigs.put(StreamsConfig.EXACTLY_ONCE, true);

    final Topology topology = buildTopology();

    out.println(KafkaStreamsTopologyGraphvizPrinter.print(topology.describe()));

    kafkaStreams = new KafkaStreams(topology, streamsConfigs);
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .<String, String>stream("text")
        .peek((k, v) -> out.println(v))
        .flatMapValues(line -> Arrays.asList(line.split("\\s+")))
        .peek((k, v) -> out.println(v))
        .groupBy((k, word) -> word)
        .count()
        .toStream()
        .peek((k, v) -> out.println(v))
        .to("word-count", Produced.valueSerde(Serdes.Long()));
    return builder.build();
  }

  private void start() throws ExecutionException, InterruptedException {
    final Map<NewTopic, List<ConfigEntry>> topics = new HashMap<>();
    topics.put(new NewTopic("text", 3, (short) 1), Collections.emptyList());
    topics.put(new NewTopic("word-count", 3, (short) 1), Collections.emptyList());
    TopicsApp.createTopics(CommonProperties.BOOTSTRAP_SERVERS, topics);

    kafkaStreams.start();
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final WordCountStreamsApp app = new WordCountStreamsApp();

    app.start();
  }
}
