package no.sysco.middleware.workshop.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class WordCountStreamsAppTest {

  private TopologyTestDriver testDriver;

  @Before
  public void setUp() {
    WordCountStreamsApp app = new WordCountStreamsApp();
    Topology topology = app.buildTopology();
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test" + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    testDriver = new TopologyTestDriver(topology, props);
  }

  @Test
  public void test() {

    final ConsumerRecordFactory<String, String> consumerRecordFactory =
        new ConsumerRecordFactory<>(
            "text",
            new StringSerializer(),
            new StringSerializer());

    testDriver.pipeInput(consumerRecordFactory.create(
        Arrays.asList(
            KeyValue.pair("", "aaa bbb ccc"),
            KeyValue.pair("", "aaa bbb"),
            KeyValue.pair("", "aaa")
        )
    ));

//    testDriver.advanceWallClockTime(1000L);

    //First round
    ProducerRecord<String, Long> record1 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("aaa", record1.key());
    assertEquals(Long.valueOf(1), record1.value());
    ProducerRecord<String, Long> record2 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("bbb", record2.key());
    assertEquals(Long.valueOf(1), record2.value());
    ProducerRecord<String, Long> record3 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("ccc", record3.key());
    assertEquals(Long.valueOf(1), record3.value());

    //Second round
    ProducerRecord<String, Long> record4 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("aaa", record4.key());
    assertEquals(Long.valueOf(2), record4.value());
    ProducerRecord<String, Long> record5 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("bbb", record5.key());
    assertEquals(Long.valueOf(2), record5.value());

    //Third round
    ProducerRecord<String, Long> record6 =
        testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer());
    assertEquals("aaa", record6.key());
    assertEquals(Long.valueOf(3), record6.value());
  }
}