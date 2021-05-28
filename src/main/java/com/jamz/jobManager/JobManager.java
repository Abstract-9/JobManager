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
package com.jamz.jobManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.jobManager.processors.BidProcessor;
import com.jamz.jobManager.processors.JobRequestProcessor;
import com.jamz.jobManager.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import static com.jamz.jobManager.JobManager.Constants.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class JobManager {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-job-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        final Topology topology = buildTopology(props, false);
        run_streams(topology, props);
    }

    public static Topology buildTopology(Properties props, boolean testing) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final StreamsBuilder streamBuilder = new StreamsBuilder();

        streamBuilder.table(DRONE_STATUS_TOPIC, consumed, Materialized.as(DRONE_STORE_NAME));

        final Topology topBuilder = streamBuilder.build();

        topBuilder.addSource(JOB_REQUEST_TOPIC, JOB_REQUEST_TOPIC)
                .addSource(BID_INPUT_NAME, BID_TOPIC);

        topBuilder.addProcessor(REQUEST_PROCESSOR_NAME, JobRequestProcessor::new, JOB_REQUEST_TOPIC)
                .addProcessor(BID_PROCESSOR_NAME, BidProcessor::new, BID_INPUT_NAME);

        topBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BID_STORE_NAME),
                Serdes.String(),
                jsonSerde
        ), BID_PROCESSOR_NAME);

        topBuilder.connectProcessorAndStateStores(BID_PROCESSOR_NAME, DRONE_STORE_NAME);

        topBuilder.addSink(BID_OUTPUT_NAME, BID_TOPIC, REQUEST_PROCESSOR_NAME)
                .addSink(JOB_ASSIGNMENT_TOPIC, JOB_ASSIGNMENT_TOPIC, BID_PROCESSOR_NAME);

        return topBuilder;
    }

    private static void run_streams(Topology topology, Properties props) {
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    // Processor that keeps the global store updated.
    // From: https://github.com/confluentinc/kafka-streams-examples/blob/6.1.1-post/src/main/java/io/confluent/examples/streams/GlobalStoresExample.java
    private static class GlobalStoreUpdater<K, V, KIn, KOut> implements Processor<K, V, KIn, KOut> {

        private final String storeName;

        public GlobalStoreUpdater(final String storeName) {
            this.storeName = storeName;
        }

        private KeyValueStore<K, V> store;

        @Override
        public void init(final ProcessorContext<KIn, KOut> processorContext) {
            store = processorContext.getStateStore(storeName);
        }

        @Override
        public void process(final Record<K, V> record) {
            // We are only supposed to put operation the keep the store updated.
            // We should not filter record or modify the key or value
            // Doing so would break fault-tolerance.
            // see https://issues.apache.org/jira/browse/KAFKA-7663
            store.put(record.key(), record.value());
        }

        @Override
        public void close() {
            // No-op
        }

    }

    // The kafka streams processor api uses a lot of internal names, and we need to refer to a few external topics.
    // This just helps keep everything in one place.
    public static class Constants {
        // Topics
        public static final String JOB_REQUEST_TOPIC = "JobRequests";
        public static final String BID_TOPIC = "JobBids";
        public static final String DRONE_STATUS_TOPIC = "DroneStatus";

        // Internal names
        public static final String JOB_ASSIGNMENT_TOPIC = "JobAssignments";
        public static final String BID_INPUT_NAME = "BidInput";
        public static final String BID_OUTPUT_NAME = "BidOutput";
        public static final String REQUEST_PROCESSOR_NAME = "RequestProcessor";
        public static final String BID_PROCESSOR_NAME = "BidProcessor";
        public static final String DRONE_STORE_NAME = "DroneStateStore";
        public static final String BID_STORE_NAME = "BidStore";

    }
}

