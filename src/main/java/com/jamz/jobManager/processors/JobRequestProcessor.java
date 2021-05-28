package com.jamz.jobManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.jobManager.util.Location;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import static com.jamz.jobManager.JobManager.Constants.BID_OUTPUT_NAME;

public class JobRequestProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;
    private final JsonNodeFactory factory = new JsonNodeFactory(true);

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        // For dev, its ok to essentially do a stateless transformation into an internal job bid.
        // For prod, we will want to check the request is valid (using external ecommerce apis probably)
        ObjectNode bid = new ObjectNode(factory);

        // For this record, refer to JobRequestSchema.json
        JsonNode input = record.value();

        // We can use this to fake customer validation checks for now
        // TODO implement customer validation checks
        if (!input.get("requested_by").textValue().equals("valid_customer")) return;

        // This is going to be implemented as order verification, probably hooking into ecommerce api's (stripe)
        // Again, we can use it to fake validation checks in testing for now.
        // TODO implement payment validation checks
        if (!input.get("verification").textValue().equals("valid_verification")) return;

        // Calculate total trip distance
        // Drones also need to factor in the distance from their landingBay to the first location, so we provide it
        double totalDistance = totalDistanceMeters((ArrayNode) input.get("job_waypoints"));


        // With the total trip distance, we can estimate a minimum required battery level with a safety factor.
        // Until we have a better idea of what that might be, well just report the distance itself.

        if (totalDistance != 0) {
            bid.put("eventType", "AuctionOpen");
            bid.put("distance", totalDistance);
            // Copy the job waypoints over to the output record
            bid.set("job_waypoints", input.get("job_waypoints").deepCopy());

            // Everything is a go, create the bid
            context.forward(new Record<String, JsonNode>(
                    input.get("requestID").textValue(), bid, System.currentTimeMillis()),
                    BID_OUTPUT_NAME);
        }
    }

    static double totalDistanceMeters(ArrayNode waypoints) {
        Location previousLocation = null;
        double totalDistance = 0;
        for (JsonNode waypoint : waypoints) {
            if (previousLocation == null) {
                previousLocation = new Location(
                        waypoint.get("geometry").get("latitude").doubleValue(),
                        waypoint.get("geometry").get("longitude").doubleValue(),
                        0
                );
                continue;
            }
            Location current = new Location(
                    waypoint.get("geometry").get("latitude").doubleValue(),
                    waypoint.get("geometry").get("longitude").doubleValue(),
                    0
            );
            totalDistance += current.getDistanceTo(previousLocation);
            previousLocation = current;
        }
        return totalDistance;
    }
}
