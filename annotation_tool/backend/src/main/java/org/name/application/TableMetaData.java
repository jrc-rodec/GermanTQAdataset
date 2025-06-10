package org.name.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetaData {

    @JsonProperty("")
    String csvId;

    @JsonProperty("code")
    String tableId;
    @JsonProperty("content")
    String content;
    @JsonProperty("filename")
    String filePath;

    public String getTableId() {
        return tableId;
    }

    public String getContent() {
        return content;
    }

    public String getFilePath() {
        return filePath;
    }
}
