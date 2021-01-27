package com.oracle.cloudsql.streamingfacade;

import com.google.gson.Gson;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.requests.*;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamingClient implements IStreamingClient {

    private final StreamClient streamClient;
    private final Gson serializer;
    private final String instanceId;
    private final String groupName;
    private final MessagePoller messagePoller;

    public StreamingClient(final String messageEndpoint, final String groupName, final String instanceId) {
        this.streamClient = getClient(messageEndpoint);
        this.serializer = new Gson();
        this.instanceId = instanceId;
        this.groupName = groupName;
        this.messagePoller = new MessagePoller(streamClient, serializer);
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
                    .value(serializer.toJson(message).getBytes(StandardCharsets.UTF_8))
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

    private CreateGroupCursorDetails getGroupCursor(String groupName, String instanceName) {
        return CreateGroupCursorDetails.builder()
                .commitOnGet(true)
                .groupName(groupName)
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .instanceName(instanceName)
                .build();
    }

    private CreateGroupCursorRequest getCreateCursorRequest(String streamId, String groupName, String instanceName) {
        return CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .opcRequestId(UUID.randomUUID().toString())
                .retryConfiguration(DefaultRetryConfigurationFactory.get())
                .createGroupCursorDetails(getGroupCursor(groupName, instanceName))
                .build();
    }

    @Override
    public void consumeMessages(String streamId, Map<String, IMessageConsumer> topicConsumer) {
        if (topicConsumer == null || topicConsumer.isEmpty()) {
            log.warn("No consumers registered");
            return;
        }

        CreateGroupCursorResponse cursorResponse = streamClient.createGroupCursor(getCreateCursorRequest(streamId, groupName, instanceId));
        String cursor = cursorResponse.getCursor().getValue();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(messagePoller.streamId(streamId).cursor(cursor).topicConsumer(topicConsumer), 0, 1, TimeUnit.SECONDS);
    }






    private StreamClient getClient(String endpoint) {
        BasicAuthenticationDetailsProvider authProvider =
                InstancePrincipalsAuthenticationDetailsProvider.builder().build();

        StreamClient streamClient = new StreamClient(authProvider);
        streamClient.setEndpoint(endpoint);
        return streamClient;
    }

    static class MessagePoller implements Runnable {
        private String streamId;
        private String cursor;
        private Map<String, IMessageConsumer> topicConsumer;
        private final StreamClient streamClient;
        private final Gson serializer;

        public MessagePoller(StreamClient streamClient, Gson serializer) {
            this.streamClient = streamClient;
            this.serializer = serializer;
        }

        public MessagePoller streamId(String streamId) {
            this.streamId = streamId;
            return this;
        }

        public MessagePoller cursor(String cursor) {
            this.cursor = cursor;
            return this;
        }

        public MessagePoller topicConsumer(Map<String, IMessageConsumer> topicConsumer) {
            this.topicConsumer = topicConsumer;
            return this;
        }


        @Override
        public void run() {
            // No error handling (there is a high chance of getting a throttling error using a tight loop)

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
                    StreamMessage msg = serializer.fromJson(_payload, StreamMessage.class);
                    consumer.consume(msg);
                } else {
                    log.warn("no consumer registered for "+topicName);
                }
            });

            cursor = getMessagesResponse.getOpcNextCursor();
            //System.out.println("cursor=" + cursor);
            //String serialized = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            //System.out.println("serialized=" + serialized);


//            ConsumerCommitResponse commitResponse = streamClient.consumerCommit(ConsumerCommitRequest.builder()
//                    .cursor(this.cursor)
//                    .streamId(streamId)
//                    .build());


        }

    }
}
