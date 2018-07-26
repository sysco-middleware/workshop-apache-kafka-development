package no.sysco.middleware.workshop.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaTweetsStreams {
  private static final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
  private static final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
  private static final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);
  //Topics
  private static final String TWEETS = "tweets";
  private static final String TWEETS_HASHTAGS = "tweets-hashtags";
  //Queryable store names
  private static final String TWEETS_BY_USERNAME = "tweets-by-username";
  private static final String HASHTAGS_COUNT = "tweets-hashtags-count";
  private static final String HASHTAG_PER_MINUTE = "tweets-hashtag-per-minute";
  private final KafkaStreams tweetsPerUserStream;
  private final KafkaStreams hashtagsCountStream;
  private final KafkaStreams hashtagPerMinuteStream;

  public KafkaTweetsStreams() {
    tweetsPerUserStream = buildTweetsPerUser();
    tweetsPerUserStream.start();

    hashtagsCountStream = buildCountHashtags();
    hashtagsCountStream.start();

    hashtagPerMinuteStream = buildHashtagProgress();
    hashtagPerMinuteStream.start();
  }

  private static KafkaStreams buildTweetsPerUser() {
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-streams-application-tweets-per-user");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(TWEETS,  Consumed.with(jsonNodeSerde, jsonNodeSerde))
        //Defining a key = screen_name
        .selectKey((key, value) ->
            value.get("payload").get("user").get("screen_name").textValue())
        //Define new value = tweets
        .mapValues(value ->
            value.get("payload").get("text").textValue())
        //Join tweets by key (i.e. screen_name)
        .groupByKey()
        //Reducing to concatenation of tweets, and keep it on TWEETS_BY_USERNAME store
        //queryable store
        .reduce((value1, value2) -> value1+"\n\n"+value2, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(TWEETS_BY_USERNAME))
        //Save values on TWEETS_BY_USERNAME topic
        .toStream()
        .to(TWEETS_BY_USERNAME, Produced.with(Serdes.String(), Serdes.String()));

    return new KafkaStreams(builder.build(), config);
  }

  private static KafkaStreams buildCountHashtags() {
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-streams-application-hashtags-count");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<JsonNode, String> hashtagsStream = builder
        .stream(TWEETS,  Consumed.with(jsonNodeSerde, jsonNodeSerde))
        .mapValues(value -> value.get("payload").get("entities").withArray("hashtags"))
        .flatMapValues(ArrayNode.class::cast)
        .mapValues(value -> value.get("text").textValue());

    hashtagsStream.to(TWEETS_HASHTAGS, Produced.with(jsonNodeSerde, Serdes.String()));

    hashtagsStream
        .groupBy((key, value)-> value, Serialized.with(Serdes.String(), Serdes.String()))
        .count()
        .toStream()
        .to(HASHTAGS_COUNT, Produced.with(Serdes.String(), Serdes.Long()));

    return new KafkaStreams(builder.build(), config);
  }

  private static KafkaStreams buildHashtagProgress() {
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-streams-application-hashtag-progress");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(TWEETS_HASHTAGS, Consumed.with(jsonNodeSerde, Serdes.String()))
        .groupBy((key, value)-> value, Serialized.with(Serdes.String(), Serdes.String()))
        //.count(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)), HASHTAG_PER_MINUTE);
        .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
        // default behavior for count() provides Serdes.Long() for value
        .count(Materialized.as(HASHTAG_PER_MINUTE));

    return new KafkaStreams(builder.build(), config);
  }

  private static <T> T waitUntilStoreIsQueryable(
      final String storeName,
      final QueryableStoreType<T> queryableStoreType,
      final KafkaStreams streams)
      throws InterruptedException {
    while (true) {
      try {
        return streams.store(storeName, queryableStoreType);
      } catch (InvalidStateStoreException ignored) {
        ignored.printStackTrace();
        // store not yet ready for querying
        Thread.sleep(1000);
      }
    }
  }

  public String getTweetsByUsername(String username) {
    try {
      final ReadOnlyKeyValueStore<String, String> store =
          waitUntilStoreIsQueryable(
              TWEETS_BY_USERNAME,
              QueryableStoreTypes.<String, String>keyValueStore(),
              tweetsPerUserStream);
      final String value = store.get(username);
      return Optional.ofNullable(value).orElse("No tweets");
    } catch (Exception e) {
      throw new IllegalStateException("Kafka State Store is not loaded", e);
    }
  }


  public String getHashtags() {
    try {
      final ReadOnlyKeyValueStore<String, Long> store =
          waitUntilStoreIsQueryable(
              HASHTAGS_COUNT,
              QueryableStoreTypes.<String, Long>keyValueStore(),
              hashtagsCountStream);
      Map<String, Long> counts =
          new HashMap<>();
      final KeyValueIterator<String, Long> all = store.all();
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
          waitUntilStoreIsQueryable(
              HASHTAG_PER_MINUTE,
              QueryableStoreTypes.windowStore(),
              hashtagPerMinuteStream);
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
}
