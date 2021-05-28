package com.jamz.jobManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.jobManager.JobManager;
import com.jamz.jobManager.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.jamz.jobManager.JobManager.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

public class JobRequestProcessorsTests {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, JsonNode> inputTopic;
    private static TestOutputTopic<String, JsonNode> outputTopic;
    private static KeyValueStore<String, JsonNode> bids;
    private static final JsonNodeFactory nodeFactory = new JsonNodeFactory(true);

    @BeforeAll
    static void setup() {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-job-manager");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        // Build the topology using the prod code.
        Topology topology = JobManager.buildTopology(props, true);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(JOB_REQUEST_TOPIC,
                Serdes.String().serializer(), jsonSerializer);
        outputTopic = testDriver.createOutputTopic(BID_TOPIC,
                Serdes.String().deserializer(), jsonDeserializer);

        bids = testDriver.getKeyValueStore(BID_STORE_NAME);

    }

    @AfterAll
    static void tearDown() {
        testDriver.close();
    }

    @Test
    void testJobRequest() {
        ObjectNode job = new ObjectNode(nodeFactory);
        job.put("requestID", "uuid")
                .put("requested_by", "valid_customer")
                .put("verification", "valid_verification")
                .set("job_waypoints", new ArrayNode(nodeFactory)
                    .add(new ObjectNode(nodeFactory)
                        .put("type", "pickup")
                        .put("estimated_weight", 5)
                        .set("geometry", new ObjectNode(nodeFactory)
                            .put("latitude", 45.420272)
                            .put("longitude", -75.680674)
                        )
                    ).add(new ObjectNode(nodeFactory)
                        .put("type", "dropoff")
                        .set("geometry", new ObjectNode(nodeFactory)
                            .put("latitude", 45.420592)
                            .put("longitude", -75.677117)
                            )
                        )
                );

        inputTopic.pipeInput("JobRequest", job);

        assertFalse(outputTopic.isEmpty());

        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals(result.key, job.get("requestID").textValue());
        assertEquals("AuctionOpen", result.value.get("eventType").textValue());
        assertTrue(278 < result.value.get("distance").doubleValue() &&
                    result.value.get("distance").doubleValue()< 283); // Should be around 280m based on our coordinates
    }
}
