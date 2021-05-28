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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static com.jamz.jobManager.JobManager.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

public class BidProcessorTests {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, JsonNode> inputTopic;
    private static TestOutputTopic<String, JsonNode> bidOutputTopic;
    private static TestOutputTopic<String, JsonNode> assignmentOutputTopic;
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

        inputTopic = testDriver.createInputTopic(BID_TOPIC,
                Serdes.String().serializer(), jsonSerializer);
        bidOutputTopic = testDriver.createOutputTopic(BID_TOPIC,
                Serdes.String().deserializer(), jsonDeserializer);
        assignmentOutputTopic = testDriver.createOutputTopic(JOB_ASSIGNMENT_TOPIC,
                Serdes.String().deserializer(), jsonDeserializer );

        bids = testDriver.getKeyValueStore(BID_STORE_NAME);

    }

    @AfterAll
    static void tearDown() {
        testDriver.close();
    }

    @BeforeEach
    void cleanStore() {
        bids.delete("uuid");
    }

    @Test
    void testBidOpen() {
        ObjectNode auction = new ObjectNode(nodeFactory);

        auction.put("eventType", "AuctionOpen")
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
        auction.put("distance", JobRequestProcessor.totalDistanceMeters((ArrayNode) auction.get("job_waypoints")));

        inputTopic.pipeInput("uuid", auction);

        assertTrue(bidOutputTopic.isEmpty());

        JsonNode storeAuction = bids.get("uuid");
        assertEquals(storeAuction.get("highest_bid").intValue(), 0);
        assertEquals(storeAuction.get("highest_bid_id").textValue(), "0x0");
        assertTrue(storeAuction.get("open_time").longValue() < System.currentTimeMillis());
    }

    @Test
    void testBidPunctuation() {
        testBidOpen(); //Pre-load the bid

        ObjectNode bid = new ObjectNode(nodeFactory);

        bid.put("eventType", "BidsPlaced")
                .set("bids", new ArrayNode(nodeFactory)
                        .add(new ObjectNode(nodeFactory)
                            .put("ID", "0x1")
                            .put("value", 50)));

        inputTopic.pipeInput("uuid", bid);

        testDriver.advanceWallClockTime(Duration.ofSeconds(11)); // It punctuates after 10s, 11 just to be sure

        // Make sure our store entry was deleted, and our assignment topic is not empty
        assertNull(bids.get("uuid"));
        assertFalse(assignmentOutputTopic.isEmpty());

        KeyValue<String, JsonNode> result = assignmentOutputTopic.readKeyValue();
        assertEquals("uuid", result.key);
        assertEquals("AuctionClose", result.value.get("eventType").textValue());
        assertEquals("0x1", result.value.get("drone_id").textValue());
    }
}
