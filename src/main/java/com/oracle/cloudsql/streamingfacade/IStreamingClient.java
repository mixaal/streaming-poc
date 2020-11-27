package com.oracle.cloudsql.streamingfacade;

import java.util.List;
import java.util.Map;

public interface IStreamingClient {
    /**
     * Publish message into stream id.
     *
     * @param streamId stream id to publish to
     * @param messages batch of messages to publish
     */
    boolean publishMessages(String streamId, List<StreamMessage> messages);


    /**
     * Consume message from the stream (from a given offset).
     *
     * @param streamId stream id
     */
    void consumeMessages(String streamId, Map<String, IMessageConsumer> topicConsumers);
}
