package com.jamz.jobManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class BidProcessor implements Processor<String, JsonNode, String, JsonNode> {


    private ProcessorContext<String, JsonNode> context;
    private final JsonNodeFactory factory = new JsonNodeFactory(true);

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        // Basically, drones that are online and capable of completing the job will say that they bid.
        // We need to take the status of that drone (from the drone status topic), convert it into a numerical bid using
        // our special formula, and choose the highest bid to do the job.
    }
}
