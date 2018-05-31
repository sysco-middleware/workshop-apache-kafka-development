package no.sysco.middleware.workshop.kafka.admin;

import no.sysco.middleware.workshop.kafka.CommonProperties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.System.out;

public class TopicsApp {

  public static void createTopics(String bootstrapServers,
                                  Map<NewTopic, List<ConfigEntry>> topicListMap)
      throws ExecutionException, InterruptedException {
    final Properties adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final AdminClient adminClient = KafkaAdminClient.create(adminProperties);

    final Set<String> existingTopic = adminClient.listTopics().names().get();

    for (Map.Entry<NewTopic, List<ConfigEntry>> newTopicAndConfig : topicListMap.entrySet()) {
      final NewTopic topic = newTopicAndConfig.getKey();
      if (!existingTopic.contains(topic.name())) {
        out.println("Topic " + topic.name() + " is been created.");
        topic.configs(
            newTopicAndConfig
                .getValue()
                .stream()
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
        final CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));
        result.all().get();
        out.println("Topic " + topic.name() + " created.");
      } else {
        Map<ConfigResource, Config> configs = new HashMap<>();
        final Config config = new Config(newTopicAndConfig.getValue());
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()), config);
        adminClient.alterConfigs(configs).all().get();
        out.println("Topic " + topic.name() + " has been updated.");
      }
    }
    adminClient.close();
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Map<NewTopic, List<ConfigEntry>> topics = new HashMap<>();
    topics.put(new NewTopic("topic-1", 3, (short) 1),
        Arrays.asList(
            new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(50 *1024 * 1024)),
            new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, String.valueOf(Long.MAX_VALUE))));
    TopicsApp.createTopics(CommonProperties.BOOTSTRAP_SERVERS, topics);
  }
}