package com.oracle.cloudsql.streamingfacade;

import com.google.gson.Gson;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

@Slf4j
public class StreamingClient implements IStreamingClient {

    private final StreamClient streamClient;
    private String cursor;
    private final Gson serialializer ;
    private Long readOffset;

    public StreamingClient(final String messageEndpoint) {
        this(messageEndpoint, null);
    }

    public StreamingClient(final String messageEndpoint, String cursor) {
        this.readOffset = 0L;
        this.streamClient = getClient(messageEndpoint);
        this.cursor = cursor;
        this.serialializer = new Gson();
        if(cursor!=null && !cursor.isEmpty()) {
            String cursorDecoded = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            try {
                final CursorSerialization serializedCursor = serialializer.fromJson(cursorDecoded, CursorSerialization.class);
                this.readOffset = serializedCursor.getOffset();
            }
            catch (Throwable t) {
                log.warn(t.getMessage(), t);
            }
        }
    }

    @Override
    public void consumeMessages(String streamId, IMessageConsumer consumer) {
        consumeMessages(streamId, this.readOffset, consumer);
    }

    @Override
    public boolean publishMessage(String streamId, String key, String payload) {
        if(key==null || key.isEmpty()) return false;
        if(payload==null || payload.isEmpty()) return false;
        PutMessagesResult result = streamClient.putMessages(PutMessagesRequest.builder()
                .opcRequestId(UUID.randomUUID().toString())
                .streamId(streamId)
                .putMessagesDetails(
                        PutMessagesDetails.builder()
                                .messages(Arrays.asList(PutMessagesDetailsEntry.builder()
                                        .key(key.getBytes(Charsets.UTF_8))
                                        .value(payload.getBytes(Charsets.UTF_8))
                                        .build()))
                                .build()
                )
                .build())
                .getPutMessagesResult();
        if (result.getFailures() > 0) {
            List<PutMessagesResultEntry> entries = result.getEntries();
            entries.forEach(e -> {
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
    public void consumeMessages(String streamId, Long offset, IMessageConsumer consumer) {
// No error handling
        CreateCursorDetails createCursorDetails =
                CreateCursorDetails.builder()
//                        .type(CreateCursorDetails.Type.TrimHorizon) // All messages from the beginning
//                        .type(CreateCursorDetails.Type.Latest) // reads messages only while online
                        .type(CreateCursorDetails.Type.AfterOffset)
                        // Possible error: BmcException: (400, InvalidParameter, false) Offset 8 is invalid
                        .offset(offset) //After offset 29
                        .partition("0")
                        .build();
// If using AT_OFFSET or AFTER_OFFSET you need to specify the offset [builder].offset(offset)
// If using AT_TIME you need to specify the time [builder].time(new Date(xxx))

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(createCursorDetails)
                        .build();

        this.cursor = streamClient.createCursor(createCursorRequest).getCursor()
                .getValue();

        // No error handling (there is a high chance of getting a throttling error using a tight loop)
        while (true) { // or your own exit condition
            GetMessagesRequest getMessagesRequest =
                    GetMessagesRequest.builder()
                            .streamId(streamId)
                            .limit(5)
                            .cursor(this.cursor)
                            .build();

            GetMessagesResponse getMessagesResponse = streamClient.getMessages(getMessagesRequest);

            // This could be empty, but we will always return an updated cursor
            getMessagesResponse.getItems().forEach(message -> {
                // Process the message
                String key = new String(message.getKey(), Charsets.UTF_8);
                String value = new String(message.getValue(), Charsets.UTF_8);
                System.out.println("key=" + key + ", value=" + value);
            });
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException ex) {

            }
            cursor = getMessagesResponse.getOpcNextCursor();
            System.out.println("cursor=" + cursor);
            String serialized = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            System.out.println("serialized=" + serialized);
        }

    }

    public void listStreams(StreamAdminClient adminClient, String tenancy) {
        // No error handling
        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder()
                        .compartmentId(tenancy)
                        .build();
        String page;
        do {
            ListStreamsResponse listStreamsResponse = adminClient.listStreams(listStreamsRequest);
            List<StreamSummary> streams = listStreamsResponse.getItems();
            // Do something with the streams
            for (StreamSummary summary : streams) {
                String id = summary.getId();
                StreamSummary.LifecycleState state = summary.getLifecycleState();
                System.out.println("id: " + id);
                System.out.println("state: " + state);
            }
            page = listStreamsResponse.getOpcNextPage();
        } while (page != null);
    }

    private StreamClient getClient(String endpoint) {
        BasicAuthenticationDetailsProvider authProvider =
                InstancePrincipalsAuthenticationDetailsProvider.builder().build();
//        StreamAdminClient adminClient = new StreamAdminClient(authProvider);
//        adminClient.setEndpoint(ADMIN_ENDPOINT); // You cannot use the setRegion method
//        listStreams(adminClient, "");
//        GetStreamRequest getStreamRequest = GetStreamRequest.builder().streamId(streamId).build();
//        Stream stream = adminClient.getStream(getStreamRequest).getStream();
        //String streamId = "mixaal";


        StreamClient streamClient = new StreamClient(authProvider);
        streamClient.setEndpoint(endpoint);
        return streamClient;
    }

}
