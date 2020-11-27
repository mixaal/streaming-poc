package com.oracle.cloudsql.streamingfacade;

import com.google.gson.Gson;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.requests.*;
import com.oracle.bmc.streaming.responses.ConsumerCommitResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.cloudsql.streamingfacade.objectstore.ICursorStorage;
import com.oracle.cloudsql.streamingfacade.objectstore.ObjectStoreCursorStorageImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Slf4j
public class StreamingClient implements IStreamingClient {

    private final StreamClient streamClient;
    private final Gson serialializer;
    private final ICursorStorage cursorStorage;
    private final String clientId;

    public StreamingClient(final String messageEndpoint, final String clientId, ICursorStorage cursorStorage) {
        this.cursorStorage = cursorStorage;
        this.streamClient = getClient(messageEndpoint);
        this.serialializer = new Gson();
        this.clientId = clientId;
    }

    //FIXME use batch publish!!!
    @Override
    public boolean publishMessages(String streamId, List<StreamMessage> messages) {
        List<PutMessagesDetailsEntry> entries = new ArrayList<>();
        for(StreamMessage message: messages) {
            if(message.getPayload()==null) continue;
            if(message.getCreatedAt()==null) message.setCreatedAt(Instant.now().toEpochMilli());
            if(message.getTopic()==null || message.getTopic().isEmpty()) message.setTopic("DEFAULT");
            if(message.getUuid()==null || message.getUuid().isEmpty()) message.setUuid(UUID.randomUUID().toString());

            entries.add(PutMessagesDetailsEntry.builder()
                    .key(message.getTopic().getBytes(StandardCharsets.UTF_8))
                    .value(serialializer.toJson(message).getBytes(StandardCharsets.UTF_8))
                    .build());

        }

        PutMessagesResult result = streamClient.putMessages(PutMessagesRequest.builder()
                .opcRequestId(UUID.randomUUID().toString())
                .streamId(streamId)
                .retryConfiguration(DefaultRetryConfigurationFactory.get())
                .putMessagesDetails(
                        PutMessagesDetails.builder()
                                .messages(entries)
                                .build()
                )
                .build())
                .getPutMessagesResult();
        if (result.getFailures() > 0) {
            List<PutMessagesResultEntry> failures = result.getEntries();
            failures.forEach(e -> {
                if (!StringUtils.isEmpty(e.getError())) {
                    log.warn(e.getError());
                }
            });
            return false;
        } else {
            return true;
        }
    }



    @Override
    public void consumeMessages(String streamId, Map<String, IMessageConsumer> topicConsumers) {
        String cursor = cursorStorage.get(clientId, streamId);
        Long offset = decodeCursor(cursor);
        consumeMessages(streamId, offset, topicConsumers);
    }

    private String getCursorToReadAllMessages(String streamId) {
        CreateCursorDetails createCursorDetails =
                CreateCursorDetails.builder()
                        .type(CreateCursorDetails.Type.TrimHorizon)
                        .partition("0")
                        .build();

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(createCursorDetails)
                        .build();

        return streamClient.createCursor(createCursorRequest).getCursor()
                .getValue();
    }

    private String getCursorForOnlineProcessing(String streamId) {
        CreateCursorDetails createCursorDetails =
                CreateCursorDetails.builder()
                        .type(CreateCursorDetails.Type.Latest) // reads messages only while online
                        .partition("0")
                        .build();

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(createCursorDetails)
                        .build();

        return streamClient.createCursor(createCursorRequest).getCursor()
                .getValue();
    }

    private String getCursorRequest(String streamId, Long offset) {
        CreateCursorDetails createCursorDetails =
                CreateCursorDetails.builder()
                        .type(CreateCursorDetails.Type.AfterOffset)
                        // Possible error: BmcException: (400, InvalidParameter, false) Offset 8 is invalid
                        .offset(offset) //After offset 29
                        .partition("0")
                        .build();

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(createCursorDetails)
                        .build();

        return streamClient.createCursor(createCursorRequest).getCursor()
                .getValue();

    }

    private void consumeMessages(String streamId, Long offset, Map<String, IMessageConsumer> topicConsumer) {
        if(topicConsumer==null || topicConsumer.isEmpty()) {
            log.warn("No consumers registered");
            return;
        }
        String cursor = null;
        if(offset!=null) {
            try {
                cursor = getCursorRequest(streamId, offset);
            } catch (BmcException ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
        if(cursor==null) {
            cursor = getCursorToReadAllMessages(streamId);
        }

        // No error handling (there is a high chance of getting a throttling error using a tight loop)
        while (true) { // or your own exit condition
            GetMessagesRequest getMessagesRequest =
                    GetMessagesRequest.builder()
                            .streamId(streamId)
                            .limit(5)
                            .cursor(cursor)
                            .build();

            GetMessagesResponse getMessagesResponse = streamClient.getMessages(getMessagesRequest);


            // This could be empty, but we will always return an updated cursor
            getMessagesResponse.getItems().forEach(message -> {
                // Process the message
                String topicName = new String(message.getKey(), StandardCharsets.UTF_8);
                IMessageConsumer consumer = topicConsumer.get(topicName);
                if(consumer!=null) {
                    String _payload = new String(message.getValue(), StandardCharsets.UTF_8);
                    StreamMessage msg = serialializer.fromJson(_payload, StreamMessage.class);
                    consumer.consume(msg);
                } else {
                    log.warn("no consumer registered for "+topicName);
                }
            });

            cursor = getMessagesResponse.getOpcNextCursor();
            System.out.println("cursor=" + cursor);
            String serialized = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            System.out.println("serialized=" + serialized);

            //FIXME async? Sleeping there anyway - shoudl react on retry strategy error
            //FIXME what if we are unable to store the cursor?
            try {
                cursorStorage.store(clientId, streamId, cursor);
            }
            catch (Throwable t) {
                log.error(t.getMessage(), t);
            }

//            ConsumerCommitResponse commitResponse = streamClient.consumerCommit(ConsumerCommitRequest.builder()
//                    .cursor(this.cursor)
//                    .streamId(streamId)
//                    .build());

            try {
                Thread.sleep(1_000);
            } catch (InterruptedException ex) {

            }
        }

    }

    private StreamClient getClient(String endpoint) {
        BasicAuthenticationDetailsProvider authProvider =
                InstancePrincipalsAuthenticationDetailsProvider.builder().build();

        StreamClient streamClient = new StreamClient(authProvider);
        streamClient.setEndpoint(endpoint);
        return streamClient;
    }

    private Long decodeCursor(String cursor) {
        if (cursor != null && !cursor.isEmpty()) {
            String cursorDecoded = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            try {
                final CursorSerialization serializedCursor = serialializer.fromJson(cursorDecoded, CursorSerialization.class);
                return serializedCursor.getOffset();
            } catch (Throwable t) {
                log.warn(t.getMessage(), t);
            }
        }
        return null;
    }

}
