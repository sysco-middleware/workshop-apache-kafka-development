package no.sysco.middleware.workshop.kafka.streams;

import org.apache.kafka.streams.TopologyDescription;

import java.io.StringWriter;
import java.util.stream.Stream;

/**
 *
 */
public class KafkaStreamsTopologyGraphvizPrinter {

    public static String print(TopologyDescription topologyDescription) {
        StringWriter writer = new StringWriter();
        writer.append("digraph kafka_streams_topology {\n");
        writer.append("\n");

        printTopics(topologyDescription, writer);

        writer.append("\n");

        printStores(topologyDescription, writer);

        writer.append("\n");

        printSubtopologies(topologyDescription, writer);

        writer.append("}\n");
        return writer.toString();
    }

    private static void printSubtopologies(TopologyDescription topologyDescription, StringWriter writer) {
        topologyDescription
            .subtopologies()
            .forEach(subtopology -> printSubtopology(subtopology, writer));
    }

    private static void printStores(TopologyDescription topologyDescription, StringWriter writer) {
        topologyDescription.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .filter(node -> node instanceof TopologyDescription.Processor)
            .flatMap(node -> ((TopologyDescription.Processor) node).stores().stream())
            .distinct()
            .forEach(topic ->
                writer.append("  \"store-").append(topic).append("\" [shape=box3d, label=\"Store ").append(topic).append("\"];\n"));
    }

    private static void printTopics(TopologyDescription topologyDescription, StringWriter writer) {
        final Stream<String> sourceTopics =
            topologyDescription.subtopologies().stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .filter(node -> node instanceof TopologyDescription.Source)
                .flatMap(node -> {
                    final String topics = ((TopologyDescription.Source) node).topics();
                    return Stream.of(topics.substring(1, topics.length() - 1).split(","));
                })
                .map(String::trim);

        final Stream<String> sinkTopics =
            topologyDescription.subtopologies().stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .filter(node -> node instanceof TopologyDescription.Sink)
                .map(node -> ((TopologyDescription.Sink) node).topic());

        Stream.concat(sourceTopics, sinkTopics)
            .distinct()
            .forEach(topic ->
                writer.append("  \"topic-").append(topic).append("\" [shape=cds, label=\"Topic ").append(topic).append("\"];\n"));
    }


    private static void printSubtopology(TopologyDescription.Subtopology subtopology, StringWriter writer) {
        writer.append("  subgraph topology_").append(String.valueOf(subtopology.id())).append(" {\n");
        writer.append("    label = \"Sub-Topology ").append(String.valueOf(subtopology.id())).append("\";\n");
        writer.append("    node [shape=ellipse];\n");

        for (TopologyDescription.Node node : subtopology.nodes()) {

            if (node instanceof TopologyDescription.Source) {
                writer.append("    \"node-").append(node.name()).append("\" [label=\"Source ").append(node.name()).append("\"];\n");

                final String topics = ((TopologyDescription.Source) node).topics();
                Stream.of(topics.substring(1, topics.length() - 1).split(","))
                    .map(String::trim)
                    .forEach(topic -> writer.append("    \"topic-").append(topic).append("\" -> \"node-").append(node.name()).append("\";\n"));


            } else if (node instanceof TopologyDescription.Processor) {
                writer.append("    \"node-").append(node.name()).append("\" [label=\"Processor ").append(node.name()).append("\"];\n");

                for (TopologyDescription.Node predecessor : node.predecessors()) {
                    writer.append("    \"node-").append(predecessor.name()).append("\" -> \"node-").append(node.name()).append("\";\n");
                }

                for (String store : ((TopologyDescription.Processor) node).stores()) {
                    writer.append("    \"node-").append(node.name()).append("\" -> \"store-").append(store).append("\";\n");
                }

            } else if (node instanceof TopologyDescription.Sink) {
                writer.append("    \"node-").append(node.name()).append("\" [label=\"Sink ").append(node.name()).append("\"];\n");

                for (TopologyDescription.Node predecessor : node.predecessors()) {
                    writer.append("    \"node-").append(predecessor.name()).append("\" -> \"node-").append(node.name()).append("\";\n");
                }

                writer.append("    \"node-").append(node.name()).append("\" -> \"topic-").append(((TopologyDescription.Sink) node).topic()).append("\";\n");
            }
        }

        writer.append("  }\n\n");
    }
}
