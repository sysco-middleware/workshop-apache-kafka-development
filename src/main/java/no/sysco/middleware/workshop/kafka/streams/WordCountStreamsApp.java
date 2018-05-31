package no.sysco.middleware.workshop.kafka.streams;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

import static java.lang.System.out;

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

  private void start() {
    kafkaStreams.start();
  }

  public static void main(String[] args) {
    final WordCountStreamsApp app = new WordCountStreamsApp();

    app.start();
  }
}
