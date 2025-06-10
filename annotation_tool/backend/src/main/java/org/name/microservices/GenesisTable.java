package org.name.microservices;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GenesisTable {
    @JsonProperty(value = "Object")
    TableContent tableContent;

    public TableContent tableContent() {
        return tableContent;
    }

    @Override
    public String toString() {
        return "GenesisTable{" +
                "tableContent=" + tableContent +
                '}';
    }
}
