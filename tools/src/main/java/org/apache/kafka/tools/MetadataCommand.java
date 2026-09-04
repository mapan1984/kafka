/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Print the complete metadata visible through the Kafka Admin API.
 */
public class MetadataCommand {

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-metadata")
            .defaultHelp(true)
            .description("Print all metadata information available from a Kafka cluster.");
        MutuallyExclusiveGroup connectionOptions = parser.addMutuallyExclusiveGroup().required(true);
        connectionOptions.addArgument("--bootstrap-server", "-b")
            .action(store())
            .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");
        parser.addArgument("--command-config", "-c")
            .action(store())
            .help("A property file containing configurations for the Admin client.");

        Namespace namespace = parser.parseArgsOrFail(args);
        Properties properties = namespace.getString("command_config") == null
            ? new Properties()
            : Utils.loadProps(namespace.getString("command_config"));
        CommandLineUtils.initializeBootstrapProperties(properties,
            Optional.ofNullable(namespace.getString("bootstrap_server")), Optional.empty());
        printMetadataResponse(System.out, fetchMetadata(properties));
    }

    private static MetadataResponseData fetchMetadata(Properties properties) throws Exception {
        AdminClientConfig config = new AdminClientConfig(properties);
        Exception lastException = null;
        for (InetSocketAddress bootstrap : ClientUtils.parseAndValidateAddresses(config)) {
            try {
                return fetchMetadata(config, bootstrap);
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw lastException;
    }

    private static MetadataResponseData fetchMetadata(AdminClientConfig config, InetSocketAddress bootstrap) throws Exception {
        Node bootstrapNode = new Node(-1, bootstrap.getHostString(), bootstrap.getPort());
        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics(time);
        NetworkClient networkClient = ClientUtils.createNetworkClient(
            config,
            "kafka-metadata",
            metrics,
            "kafka-metadata",
            new LogContext(),
            new ApiVersions(),
            time,
            1,
            config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
            new ManualMetadataUpdater(),
            new DefaultHostResolver());
        try {
            if (!NetworkClientUtils.awaitReady(networkClient, bootstrapNode, time,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG))) {
                throw new IllegalStateException("Failed to connect to " + bootstrapNode);
            }
            ClientRequest request = networkClient.newClientRequest(
                bootstrapNode.idString(), MetadataRequest.Builder.allTopics(), time.milliseconds(), true);
            ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, request, time);
            return ((org.apache.kafka.common.requests.MetadataResponse) response.responseBody()).data();
        } finally {
            networkClient.close();
            metrics.close();
        }
    }

    static void printMetadataResponse(PrintStream out, MetadataResponseData data) {
        out.println(data);
    }

    static void printMetadata(PrintStream out, Admin admin) throws Exception {
        DescribeClusterResult cluster = admin.describeCluster(
            new DescribeClusterOptions().includeAuthorizedOperations(true));
        String clusterId = cluster.clusterId().get();
        Node controller = cluster.controller().get();
        Collection<Node> nodes = cluster.nodes().get();
        Map<String, TopicDescription> topics = admin.describeTopics(
            admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get(),
            new DescribeTopicsOptions().includeAuthorizedOperations(true)
        ).allTopicNames().get();

        out.println("ClusterId: " + (clusterId == null ? "null" : clusterId));
        out.println("Controller: " + (controller == null ? "null" : formatNode(controller)));
        out.println("ClusterAuthorizedOperations: " + formatValues(cluster.authorizedOperations().get()));
        out.println("Brokers:");
        nodes.stream().sorted(Comparator.comparingInt(Node::id)).forEach(node ->
            out.println("  " + formatNode(node)));
        out.println("Topics:");
        topics.values().stream()
            .sorted(Comparator.comparing(TopicDescription::name))
            .forEach(topic -> printTopic(out, topic));
    }

    private static void printTopic(PrintStream out, TopicDescription topic) {
        out.println("  Topic: " + topic.name() + " (id=" + topic.topicId() + ", internal=" + topic.isInternal()
            + ", authorizedOperations=" + formatValues(topic.authorizedOperations()) + ")");
        List<TopicPartitionInfo> partitions = new ArrayList<>(topic.partitions());
        partitions.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
        partitions.forEach(partition -> out.println("    Partition: " + partition.partition()
            + ", leader=" + formatNodeId(partition.leader())
            + ", replicas=" + formatNodeIds(partition.replicas())
            + ", isr=" + formatNodeIds(partition.isr())
            + ", elr=" + formatNodeIds(partition.elr())
            + ", lastKnownElr=" + formatNodeIds(partition.lastKnownElr())));
    }

    private static String formatNodeIds(Collection<Node> nodes) {
        if (nodes == null) return "null";
        return nodes.stream().map(MetadataCommand::formatNodeId).collect(Collectors.joining(",", "[", "]"));
    }

    private static String formatValues(Collection<?> values) {
        if (values == null) return "null";
        return values.stream().map(Object::toString).sorted().collect(Collectors.joining(",", "[", "]"));
    }

    private static String formatNode(Node node) {
        if (node == null) return "null";
        return "id=" + node.id() + ",host=" + node.host() + ",port=" + node.port()
            + ",rack=" + (node.rack() == null ? "null" : node.rack());
    }

    private static String formatNodeId(Node node) {
        return node == null ? "null" : String.valueOf(node.id());
    }
}
