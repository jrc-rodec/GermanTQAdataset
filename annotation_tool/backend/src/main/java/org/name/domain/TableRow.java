package org.name.domain;

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;

@Entity
public class TableRow implements Serializable {
    @Column(name = "TABLE_ID")
    @Id
    String tableId;

    @ElementCollection
    /*@CollectionTable(name = "table_row_cells",     joinColumns = {
            @JoinColumn(name = "tableId", referencedColumnName = "TABLE_Id"),
            @JoinColumn(name = "rowId", referencedColumnName = "ROW_ID")
    })*/
    @Column(name = "row_entry")
    Set<TableCell> rowEntries;
    String question;
    @Id
    Integer row;
    Integer answerColumn;
    Boolean isAnswered = false;
    String content;

    public static TableRow create(String tableId,  Set<TableCell> rowEntries, String question,
                                  Integer row, Integer answerColumn, boolean isAnswered){
        TableRow tableRow = new TableRow();
        tableRow.row = row;
        tableRow.rowEntries = rowEntries;
        tableRow.answerColumn = answerColumn;
        tableRow.isAnswered = isAnswered;
        tableRow.question = question;
        tableRow.tableId = tableId;

        return tableRow;
    }

    public static TableRow create(String[] rowContents, List<String> headerNames, String tableId, int rowIndex, String content) {
        TableRow row = new TableRow();
        row.row = rowIndex;
        row.rowEntries = new HashSet<>();
        for (int i = 0; i < rowContents.length; i++) {
            row.rowEntries.add(new TableCell(headerNames.get(i), i, rowContents[i]));
        }
        row.tableId = tableId;
        row.content = content;

        return row;
    }


    public Set<TableCell> rowEntries() {
        return rowEntries;
    }

    public String tableId(){
        return tableId;
    }

    public String question() {
        return question;
    }

    public Integer row() {
        return row;
    }

    public Integer answerColumn() {
        return answerColumn;
    }

    public Boolean isAnswered() {
        return isAnswered;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableRow tableRow = (TableRow) o;

        if (!tableId.equals(tableRow.tableId)) return false;
        return row.equals(tableRow.row);
    }

    @Override
    public int hashCode() {
        int result = tableId.hashCode();
        result = 31 * result + row.hashCode();
        return result;
    }

    public void assignColumnAndQuestion(Integer answerColumn, String question) {
        isAnswered = true;
        this.answerColumn = answerColumn;
        this.question = question;
    }

    public String content() {
        return content;
    }

    /*private static StructType structType = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("tableId", DataTypes.StringType, false),
            DataTypes.createStructField("question", DataTypes.StringType, false),
            DataTypes.createStructField("row", DataTypes.IntegerType, false),
            DataTypes.createStructField("answerColumn", DataTypes.IntegerType, false),
            DataTypes.createStructField("tableRows", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
    });

    public static StructType getStructType() {
        return structType;
    }

    public Object[] getAllValues() {
        return new Object[]{tableId, question, row, answerColumn, MapType.apply(DataTypes.StringType, DataTypes.StringType)};
    }
    */
}
