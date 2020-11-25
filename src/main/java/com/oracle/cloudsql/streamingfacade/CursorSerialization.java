package com.oracle.cloudsql.streamingfacade;

import lombok.Getter;

@Getter
public class CursorSerialization {
    private String cursorType; //:"partition";
    private String type;       //:"AfterOffset","offset":29,"time":null,"partition":"0","streamId"
    private Long offset;       // 29L
    private String partition; // "0"
    private String streamId;
}
