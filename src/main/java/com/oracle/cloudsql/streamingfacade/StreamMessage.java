package com.oracle.cloudsql.streamingfacade;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@ToString
public class StreamMessage {
    private String uuid;
    private String topic;
    private Long createdAt;
    private String payload;
}
