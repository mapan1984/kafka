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
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TopicExportCommandTest {

    @Test
    public void testExportTopics() throws Exception {
        Admin admin = mock(Admin.class);
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);

        Set<String> topicNames = new HashSet<>(Arrays.asList("z-topic", "a-topic", "__internal"));
        when(admin.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(topicNames));

        Map<String, TopicDescription> descriptions = new LinkedHashMap<>();
        descriptions.put("z-topic", topic("z-topic", 3));
        descriptions.put("a-topic", topic("a-topic", 1));
        descriptions.put("__internal", topic("__internal", 2));
        when(admin.describeTopics(topicNames)).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(descriptions));

        Map<ConfigResource, Config> configs = new LinkedHashMap<>();
        configs.put(resource("z-topic"), config("86400000", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG));
        configs.put(resource("a-topic"), config("604800000", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG));
        configs.put(resource("__internal"), new Config(Collections.emptyList()));
        when(admin.describeConfigs(any())).thenReturn(describeConfigsResult);
        when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(configs));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        TopicExportCommand.exportTopics(new PrintStream(bytes), admin);

        assertEquals(
            "topic,partition_count,retention_ms\n"
                + "__internal,2,null\n"
                + "a-topic,1,null\n"
                + "z-topic,3,86400000\n",
            bytes.toString());
    }

    @Test
    public void testExportEmptyTopicList() throws Exception {
        Admin admin = mock(Admin.class);
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(admin.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        TopicExportCommand.exportTopics(new PrintStream(bytes), admin);

        assertEquals("topic,partition_count,retention_ms\n", bytes.toString());
        verify(admin, never()).describeConfigs(any());
    }

    private static TopicDescription topic(String name, int partitionCount) {
        TopicPartitionInfo partition = mock(TopicPartitionInfo.class);
        return new TopicDescription(name, name.startsWith("__"), Collections.nCopies(partitionCount, partition));
    }

    private static ConfigResource resource(String topic) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topic);
    }

    private static Config config(String value, ConfigEntry.ConfigSource source) {
        ConfigEntry entry = new ConfigEntry(
            TopicConfig.RETENTION_MS_CONFIG,
            value,
            source,
            false,
            false,
            Collections.emptyList(),
            ConfigEntry.ConfigType.LONG,
            null);
        return new Config(Arrays.asList(entry));
    }
}
