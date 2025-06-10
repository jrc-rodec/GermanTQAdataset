package org.name.microservices;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TableMetaData {
    @JsonProperty("Code")
    String code;
    @JsonProperty("Content")
    String content;
    @JsonProperty("Time")
    String time;

    public String code() {
        return code;
    }

    public String content() {
        return content;
    }

    public String time() {
        return time;
    }
}
