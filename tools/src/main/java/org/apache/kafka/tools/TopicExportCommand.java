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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Export topic names, partition counts, and explicitly configured retention times as CSV.
 */
public class TopicExportCommand {

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
            .newArgumentParser("kafka-topic-export")
            .defaultHelp(true)
            .description("Export topic names, partition counts, and explicitly configured retention times as CSV.");
        parser.addArgument("--bootstrap-server", "-b")
            .required(true)
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
            Optional.of(namespace.getString("bootstrap_server")), Optional.empty());

        try (Admin admin = Admin.create(properties)) {
            exportTopics(System.out, admin);
        }
    }

    static void exportTopics(PrintStream out, Admin admin) throws Exception {
        Set<String> topicNames = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        out.println("topic,partition_count,retention_ms");
        if (topicNames.isEmpty()) {
            return;
        }

        Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames).allTopicNames().get();
        List<ConfigResource> resources = topicNames.stream()
            .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
            .collect(Collectors.toList());
        Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get();

        descriptions.values().stream()
            .sorted(Comparator.comparing(TopicDescription::name))
            .forEach(description -> printTopic(out, description, configs));
    }

    private static void printTopic(PrintStream out,
                                   TopicDescription description,
                                   Map<ConfigResource, Config> configs) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, description.name());
        Config config = configs.get(resource);
        ConfigEntry retention = config == null ? null : config.get(TopicConfig.RETENTION_MS_CONFIG);
        String retentionMs = retention != null
            && retention.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
            ? retention.value()
            : null;
        out.printf("%s,%d,%s%n", description.name(), description.partitions().size(), retentionMs);
    }
}
