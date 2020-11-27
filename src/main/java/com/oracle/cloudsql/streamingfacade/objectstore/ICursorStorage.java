package com.oracle.cloudsql.streamingfacade.objectstore;

public interface ICursorStorage {
    void store(String clientId, String streamId, String serializedCursor);
    String get(String clientId, String streamId);
}
