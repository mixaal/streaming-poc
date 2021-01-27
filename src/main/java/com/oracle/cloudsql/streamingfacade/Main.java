package com.oracle.cloudsql.streamingfacade;

import java.util.*;

public class Main {

    public static void main(String []args) {
        if(args.length!=2) {
            System.err.println("Usage\n java -jar <.jar> consumer <clientId>\nor:\n java -jar <.jar> producer <numberOfMessages>");
        }
        int numOfMessages = 0;
        String clientId= "";

        boolean producer = "producer".equals(args[0].toLowerCase());
        if(producer) {
            numOfMessages = new Integer(args[1]);
        } else {
            clientId= args[1];
        }
        String endpoint = "https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com";
        // cloudsql-metering - 2 partitions - test-oke - oraclebigdatadb
        String streamId = "ocid1.stream.oc1.phx.amaaaaaayrywvyya44k27kifwmwezu3dumu3xtrgyyos27tgixs7kilbr3qq";
        StreamingClient client = new StreamingClient(
                endpoint,
                "metering-services",
                clientId
        );
        if(producer) {
            List<StreamMessage> messages = new ArrayList<>();
            for (int i=0; i<numOfMessages; i++) {
                messages.add(
                        new StreamMessage.StreamMessageBuilder()
                                .topic(i<6 ? "metering" : "events")
                                .payload("ahojda: "+i)
                                .build()
                );
            }
            client.publishMessages(streamId, messages);
        } else {
            Map<String, IMessageConsumer> consumers = new HashMap<>();
            consumers.put("metering", message -> System.out.println("I'm billing: "+message));
            consumers.put("events", message -> System.out.println("I'm events: "+message));
            client.consumeMessages(
                    streamId,
                    consumers
            );
        }
    }
}
