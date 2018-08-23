package no.sysco.middleware.workshop.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.lang.System.out;

/**
 *
 */
public class KafkaTweetsStreams {
  private static final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
  private static final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
  private static final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);
  //Topics
  private static final String TWEETS = "twitter_json_01";
  //Queryable store names
  private static final String HASHTAGS_COUNT = "tweets_hashtags_count_02";
  private static final String HASHTAG_PER_MINUTE = "tweets_hashtag_per_minute_02";

  //  private final KafkaStreams kafkaStreams;
  private final KafkaStreams hashtagsStream;


  KafkaTweetsStreams() {
    Properties config1 = new Properties();
    config1.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-streams-v02");
    config1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CommonProperties.BOOTSTRAP_SERVERS);
    config1.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams0");
    config1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Topology.AutoOffsetReset.EARLIEST.name().toLowerCase());
    hashtagsStream = new KafkaStreams(buildCountHashtags(), config1);

  }

  void start() {
    hashtagsStream.start();
  }

  void stop() {
    hashtagsStream.cleanUp();

    hashtagsStream.close();
  }

  private Topology buildCountHashtags() {
    final StreamsBuilder builder = new StreamsBuilder();

    final KGroupedStream<String, String> stream = builder
        .stream(TWEETS, Consumed.with(jsonNodeSerde, jsonNodeSerde))
        .mapValues(value -> value.withArray("HashtagEntities"))
        .flatMapValues(ArrayNode.class::cast)
        .mapValues(value -> value.get("Text").textValue())
        .mapValues((ValueMapper<String, String>) String::toLowerCase)
        .groupBy((key, value) -> value, Serialized.with(Serdes.String(), Serdes.String()));

    stream
        .count(Materialized.as(HASHTAGS_COUNT));

    stream
        .windowedBy(TimeWindows.of(Duration.ofMinutes(1).toMillis()))
        // default behavior for count() provides Serdes.Long() for value
        .count(Materialized.as(HASHTAG_PER_MINUTE));
    return builder.build();
  }

  String getHashtags() {
    try {
      Map<String, Long> counts = new HashMap<>();
      final KeyValueIterator<String, Long> all =
          hashtagsStream.store(HASHTAGS_COUNT, QueryableStoreTypes.<String, Long>keyValueStore()).all();
      while (all.hasNext()) {
        KeyValue<String, Long> next = all.next();
        counts.put(next.key, next.value);
      }
      return counts.toString();
    } catch (Exception e) {
      throw new IllegalStateException("Kafka State Store is not loaded", e);
    }
  }

  public String getHashtagProgress(String hashtag) {
    try {
      ReadOnlyWindowStore<String, Long> windowStore =
          hashtagsStream.store(HASHTAG_PER_MINUTE, QueryableStoreTypes.windowStore());
      long timeFrom = 0; // beginning of time = oldest available
      long timeTo = System.currentTimeMillis(); // now (in processing-time)
      WindowStoreIterator<Long> iterator = windowStore.fetch(hashtag, timeFrom, timeTo);
      StringBuilder result = new StringBuilder();
      while (iterator.hasNext()) {
        KeyValue<Long, Long> next = iterator.next();
        long windowTimestamp = next.key;
        result.append(String.format("\nHashtag '%s' @ time %s is %d%n", hashtag, new Date(windowTimestamp).toString(), next.value));
      }
      return result.toString();
    } catch (Exception e) {
      throw new IllegalStateException("Kafka State Store is not loaded", e);
    }
  }

  public static void main(String[] args) {
    KafkaTweetsStreams streams = new KafkaTweetsStreams();
    streams.hashtagsStream.setUncaughtExceptionHandler((thread, throwable) -> throwable.printStackTrace());

    streams.start();
  }
}
