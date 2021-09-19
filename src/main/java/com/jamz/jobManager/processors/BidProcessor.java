package com.jamz.jobManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.jamz.jobManager.JobManager.Constants.*;

public class BidProcessor implements Processor<String, JsonNode, String, JsonNode> {


    private ProcessorContext<String, JsonNode> context;
    private final JsonNodeFactory factory = new JsonNodeFactory(true);
    private KeyValueStore<String, JsonNode> store;

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        Processor.super.init(context);
        this.context = context;
        this.store = context.getStateStore(BID_STORE_NAME);

        // Schedule job assignment resolutions from open bids. It's scheduled to happen ~10s after the bid opens, but
        // this will obviously become adjusted to something more fitting.
        this.context.schedule(Duration.ofSeconds(1L), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, JsonNode> iter = this.store.all();

            while(iter.hasNext()) {
                KeyValue<String, JsonNode> auction = iter.next();
                JsonNode value = auction.value;

                // If the auction has been open for 10 seconds, try to punctuate. Otherwise, wait for more bids and
                // shift the open time.
                if (TimeUnit.MILLISECONDS.toSeconds(timestamp - value.get("open_time").longValue()) >= 10) {
                    if (value.get("highest_bid").intValue() > 0) {
                        ObjectNode result = new ObjectNode(factory);
                        result.put("drone_id", value.get("highest_bid_id").textValue())
                                .put("eventType", "JobAssignmentRequest")
                                .set("job_waypoints", value.get("job_waypoints"));
                        this.context.forward(new Record<String, JsonNode>(
                                auction.key, result, System.currentTimeMillis()
                        ), JOB_ASSIGNMENT_TOPIC);
                        this.store.delete(auction.key);
                    }
                }
            }
        });
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        // Basically, drones that are online and capable of completing the job will say that they bid.
        // We need to take the status of that drone (from the drone status topic), convert it into a numerical bid using
        // our special formula, and choose the highest bid to do the job.
        if (!record.value().has("eventType")) return;
        String eventType = record.value().get("eventType").textValue();

        if(eventType.equals("AuctionOpen")) {
            ObjectNode auction = new ObjectNode(factory);

            auction.put("highest_bid", 0);
            auction.put("highest_bid_id", "0x0");
            auction.put("open_time", record.timestamp());
            auction.set("job_waypoints", record.value().get("job_waypoints"));

            this.store.put(record.key(), auction);
        } else if (eventType.equals("BidsPlaced")) {
            int highestBid = 0;
            String highestBidID = null;
            for (JsonNode bid : record.value().get("bids")) {
                if (bid.get("value").intValue() > highestBid) {
                    highestBid = bid.get("value").intValue();
                    highestBidID = bid.get("ID").textValue();
                }
            }
            ObjectNode auction = (ObjectNode) this.store.get(record.key());
            if (highestBidID != null && highestBid > auction.get("highest_bid").intValue()) {
                auction.put("highest_bid", highestBid);
                auction.put("highest_bid_id", highestBidID);
                this.store.put(record.key(), auction);
            }
        }
    }
}
