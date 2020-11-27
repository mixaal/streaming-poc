package com.oracle.cloudsql.streamingfacade;

public interface IMessageConsumer {
    /**
     * Consume message from streaming service.
     *
     * @param key  message key
     * @param payload message payload
     */
    void consume(StreamMessage message);
}
