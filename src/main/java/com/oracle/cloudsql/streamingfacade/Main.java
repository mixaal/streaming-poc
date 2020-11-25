package com.oracle.cloudsql.streamingfacade;

public class Main {

    public static void main(String []args) {
        String endpoint = "https://cell-1.streaming.us-ashburn-1.oci.oraclecloud.com";
        // pixaal - 1 partition - test-oke - oraclebigdatadb
        String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyya5kyz37krqusyzq62oehq24lm7me6sagl7ntcetqrkocq";
        // mixaal - 3 partitions - test-oke - oraclebigdatadb
        // String streamId = "ocid1.stream.oc1.iad.amaaaaaayrywvyyalpdke4kwwkf7ua6nebekynheqr3cyvv2kvu7t5bdeoja;
        StreamingClient client = new StreamingClient(
                endpoint,
                // from Nov-25
                "eyJjdXJzb3JUeXBlIjoicGFydGl0aW9uIiwidHlwZSI6IkFmdGVyT2Zmc2V0Iiwib2Zmc2V0IjoyOSwidGltZSI6bnVsbCwicGFydGl0aW9uIjoiMCIsInN0cmVhbUlkIjoib2NpZDEuc3RyZWFtLm9jMS5pYWQuYW1hYWFhYWF5cnl3dnl5YTVreXozN2tycXVzeXpxNjJvZWhxMjRsbTdtZTZzYWdsN250Y2V0cXJrb2NxIiwiZXhwaXJhdGlvbiI6MTYwNjMyOTg1MTg5OCwiY3Vyc29yVHlwZSI6InBhcnRpdGlvbiJ9"
        );
        if(args.length>0) {
            Integer numOfMessages = new Integer(args[0]);
            for (int i=0; i<numOfMessages; i++) {
                client.publishMessage(streamId, "EVENT_"+i, "hello darling: "+i);
            }
        } else {
            client.consumeMessages(
                    streamId,
                    (key, payload) -> System.out.println("key="+key+"  payload="+payload)
            );
        }
    }
}
