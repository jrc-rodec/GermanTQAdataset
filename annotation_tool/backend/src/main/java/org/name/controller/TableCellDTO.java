package org.name.controller;

public class TableCellDTO {
    String cellValue;
    String rowName;
    Integer columnId;

    public TableCellDTO(String rowName, Integer columnId, String cellValue) {
        this.cellValue = cellValue;
        this.rowName = rowName;
        this.columnId = columnId;
    }

    public String getCellValue() {
        return cellValue;
    }

    public String getRowName() {
        return rowName;
    }

    public Integer getColumnId() {
        return columnId;
    }
}
