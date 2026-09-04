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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.message.MetadataResponseData;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataCommandTest {

    @Test
    public void testPrintAllMetadata() throws Exception {
        Node broker0 = new Node(0, "broker-0", 9092, "rack-a");
        Node broker1 = new Node(1, "broker-1", 9092, "rack-b");
        MockAdminClient admin = new MockAdminClient.Builder()
            .clusterId("test-cluster")
            .brokers(Arrays.asList(broker1, broker0))
            .controller(1)
            .build();
        admin.addTopic(true, "__internal", Collections.singletonList(new TopicPartitionInfo(
            0, broker0, Arrays.asList(broker0, broker1), Collections.singletonList(broker0),
            Collections.singletonList(broker1), Collections.emptyList()
        )), Collections.emptyMap());
        admin.addTopic(false, "z-topic", Collections.singletonList(new TopicPartitionInfo(
            1, broker1, Arrays.asList(broker1, broker0), Arrays.asList(broker1, broker0),
            Collections.emptyList(), Collections.singletonList(broker0)
        )), Collections.emptyMap());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        MetadataCommand.printMetadata(new PrintStream(bytes), admin);
        String output = bytes.toString();

        assertTrue(output.contains("ClusterId: test-cluster"));
        assertTrue(output.contains("Controller: id=0,host=broker-0,port=9092,rack=rack-a"));
        assertTrue(output.contains("ClusterAuthorizedOperations: []"));
        assertTrue(output.indexOf("  id=0,host=broker-0") < output.indexOf("  id=1,host=broker-1"));
        assertTrue(output.indexOf("  Topic: __internal") < output.indexOf("  Topic: z-topic"));
        assertTrue(output.contains("internal=true"));
        assertTrue(output.contains("authorizedOperations=[]"));
        assertTrue(output.contains("leader=0, replicas=[0,1], isr=[0], elr=[1], lastKnownElr=[]"));
        assertTrue(output.contains("leader=1, replicas=[1,0], isr=[1,0], elr=[], lastKnownElr=[0]"));
    }

    @Test
    public void testPrintMetadataResponseIncludesProtocolFields() {
        MetadataResponseData data = new MetadataResponseData()
            .setThrottleTimeMs(7)
            .setClusterId("cluster")
            .setControllerId(1)
            .setClusterAuthorizedOperations(42);
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1).setHost("host").setPort(9092).setRack("rack"));
        data.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setErrorCode((short) 4).setName("topic").setIsInternal(true)
            .setTopicAuthorizedOperations(43)
            .setPartitions(Collections.singletonList(new MetadataResponseData.MetadataResponsePartition()
                .setErrorCode((short) 5).setPartitionIndex(0).setLeaderId(1).setLeaderEpoch(9)
                .setReplicaNodes(Collections.singletonList(1)).setIsrNodes(Collections.singletonList(1))
                .setOfflineReplicas(Collections.singletonList(2)))));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        MetadataCommand.printMetadataResponse(new PrintStream(bytes), data);
        String output = bytes.toString();
        assertTrue(output.contains("throttleTimeMs=7"));
        assertTrue(output.contains("topicAuthorizedOperations=43"));
        assertTrue(output.contains("leaderEpoch=9"));
        assertTrue(output.contains("offlineReplicas=[2]"));
    }
}
