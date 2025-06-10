package org.name.domain;


import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;

@Embeddable
public class TableCell {
    String rowName;
    Integer columnId;
    String cellValue;

    public TableCell(String rowName, Integer columnId, String cellValue) {
        this.rowName = rowName;
        this.columnId = columnId;
        this.cellValue = cellValue;
    }

    public TableCell() {

    }


    public String getRowName() {
        return rowName;
    }

    public Integer getColumnId() {
        return columnId;
    }

    public String getCellValue() {
        return cellValue;
    }
}
