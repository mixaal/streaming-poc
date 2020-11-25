package com.oracle.cloudsql.streamingfacade;

public interface IStreamingClient {
    /**
     * Publish message into stream id.
     *
     * @param streamId stream id to publish to
     * @param key key to use
     * @param payload message payload
     */
    boolean publishMessage(String streamId, String key, String payload);

    /**
     * Consume message from the stream (from a given offset).
     *
     * @param streamId stream id
     * @param offset offset to start with
     */
    void consumeMessages(String streamId, Long offset, IMessageConsumer consumer);

    /**
     * Consume message from the stream (from a given offset).
     *
     * @param streamId stream id
     */
    void consumeMessages(String streamId, IMessageConsumer consumer);
}
