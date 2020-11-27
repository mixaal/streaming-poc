package com.oracle.cloudsql.streamingfacade;

import com.oracle.bmc.Region;
import com.oracle.cloudsql.streamingfacade.objectstore.ObjectStoreCursorStorageImpl;
import java.util.*;

public class Main {

    public static void main(String []args) {
        String objectStoreCompartmentId = "ocid1.compartment.oc1..aaaaaaaawdyzbt7uzg7pjlfbbwqamcbnsyol7kle4tdevhdm3icqpfubxjwa";
        String endpoint = "https://cell-1.streaming.us-ashburn-1.oci.oraclecloud.com";
        String clientId = "resource-manager-22";
        clientId = "new-client-must-read-everything";
        // pixaal - 1 partition - test-oke - oraclebigdatadb
        String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyya5kyz37krqusyzq62oehq24lm7me6sagl7ntcetqrkocq";
        // mixaal - 3 partitions - test-oke - oraclebigdatadb
        // String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyyalpdke4kwwkf7ua6nebekynheqr3cyvv2kvu7t5bdeoja;
        StreamingClient client = new StreamingClient(
                endpoint,
                clientId,
                new ObjectStoreCursorStorageImpl(Region.US_PHOENIX_1, objectStoreCompartmentId)
        );
        if(args.length>0) {
            int numOfMessages = new Integer(args[0]);
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
