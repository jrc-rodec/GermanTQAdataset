package org.name.microservices;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TablesAnswer {
    @JsonProperty("List")
    List<TableMetaData> tableMetaDataList;

    public List<TableMetaData> tableMetaDataList() {
        return tableMetaDataList;
    }
}
