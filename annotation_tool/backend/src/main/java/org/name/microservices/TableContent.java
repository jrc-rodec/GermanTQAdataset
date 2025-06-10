package org.name.microservices;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableContent {

    @JsonProperty(value = "Content")
    String content;

    public String content() {
        return content;
    }
}
