package com.oracle.cloudsql.streamingfacade;

import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.requests.*;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;


public class Main {
    private static final String ENDPOINT = "auth.ushburn";

    public void publishMessage(StreamClient streamClient, String streamId, Integer numOfMessages) {
        if(numOfMessages==null) return;
        for(int i=0; i<numOfMessages; i++) {
            String key = "EVENT_" + i;
            PutMessagesResult result = streamClient.putMessages(PutMessagesRequest.builder()
                    .opcRequestId("blabla")
                    .streamId(streamId)
                    .putMessagesDetails(
                            PutMessagesDetails.builder()
                                    .messages(Arrays.asList(PutMessagesDetailsEntry.builder()
                                            .key(key.getBytes(Charsets.UTF_8))
                                            .value("delete whatever you think".getBytes(Charsets.UTF_8))
                                            .build()))
                                    .build()
                    )
                    .build())
                    .getPutMessagesResult();
            if (result.getFailures() > 0) {
                List<PutMessagesResultEntry> entries = result.getEntries();
                entries.forEach(e -> {
                    if (!StringUtils.isEmpty(e.getError())) {
                        System.out.println(e.getError());
                    }
                });
            }
        }
    }

    public void consumeMessages(StreamClient streamClient, String streamId) {
// No error handling
        CreateCursorDetails createCursorDetails =
                CreateCursorDetails.builder()
//                        .type(CreateCursorDetails.Type.TrimHorizon) // All messages from the beginning
//                        .type(CreateCursorDetails.Type.Latest) // reads messages only while online
                        .type(CreateCursorDetails.Type.AfterOffset)
                        // Possible error: BmcException: (400, InvalidParameter, false) Offset 8 is invalid
                        .offset(29L) //After offset 29
                        .partition("0")
                        .build();
// If using AT_OFFSET or AFTER_OFFSET you need to specify the offset [builder].offset(offset)
// If using AT_TIME you need to specify the time [builder].time(new Date(xxx))

        CreateCursorRequest createCursorRequest =
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(createCursorDetails)
                        .build();

        String cursor = streamClient.createCursor(createCursorRequest).getCursor()
                .getValue();

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
                String key = new String(message.getKey(), Charsets.UTF_8);
                String value = new String(message.getValue(), Charsets.UTF_8);
                System.out.println("key="+key+", value="+value);
            });
            try {
                Thread.sleep(1_000);
            }catch (InterruptedException ex) {

            }
            cursor = getMessagesResponse.getOpcNextCursor();
            System.out.println("cursor="+cursor);
            String serialized = new String(Base64.getDecoder().decode(cursor), Charsets.UTF_8);
            System.out.println("serialized="+serialized);
        }

    }

    public void listStreams(StreamAdminClient adminClient, String tenancy) {
        // No error handling
        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder()
                        .compartmentId(tenancy)
                        .build();
// You can filter by OCID (exact match only) [builder].id(streamId) -> This will return 0..1 item
// You can filter by name (exact match only) [builder].name(name) -> This will return 0..n items
// You can order the result per TimeCreated or Name [builder].sortBy(SortBy.[TimeCreated|Name])
// You can change the ordering [builder].sortOrder(SortOrder.[Asc|Desc])
// You can filter by lifecycleState [builder].lifecycleState(lifecycleState)

        String page;
        do {
            ListStreamsResponse listStreamsResponse = adminClient.listStreams(listStreamsRequest);
            List<StreamSummary> streams = listStreamsResponse.getItems();
            // Do something with the streams
            for(StreamSummary summary: streams) {
                String id = summary.getId();
                StreamSummary.LifecycleState state = summary.getLifecycleState();
                System.out.println("id: "+id);
                System.out.println("state: "+state);
            }
            page = listStreamsResponse.getOpcNextPage();
        } while (page != null);
    }

    public StreamClient getClient(String endpoint) {
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

    public static void main(String []args) {
        Main main = new Main();
        String endpoint = "https://cell-1.streaming.us-ashburn-1.oci.oraclecloud.com";
        // pixaal - 1 partition - test-oke - oraclebigdatadb
        String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyya5kyz37krqusyzq62oehq24lm7me6sagl7ntcetqrkocq";
        // mixaal - 3 partitions - test-oke - oraclebigdatadb
        // String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyyalpdke4kwwkf7ua6nebekynheqr3cyvv2kvu7t5bdeoja;
        StreamClient client = main.getClient(endpoint);
        if(args.length>0) {
            Integer numOfMessages = new Integer(args[0]);
            main.publishMessage(client, streamId, numOfMessages);
        } else {
            main.consumeMessages(client, streamId);
        }
    }
}
