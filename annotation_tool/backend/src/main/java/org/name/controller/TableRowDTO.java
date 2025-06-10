package org.name.controller;

import org.name.domain.TableCell;


import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TableRowDTO {

    String tableId;
    Set<TableCellDTO> rowEntries;
    String question;
    Integer row;
    Integer answerColumn;
    String content;

    public static TableRowDTO create(String tableId, Set<TableCell> rowEntries, Integer row,
                                     String question, Integer answerColumn) {
        TableRowDTO tableRowDTO = new TableRowDTO();
        tableRowDTO.tableId = tableId;
        tableRowDTO.rowEntries = rowEntries.stream().map(
                cell -> new TableCellDTO(cell.getRowName(), cell.getColumnId(), cell.getCellValue())
        ).collect(Collectors.toSet());
        tableRowDTO.row = row;
        tableRowDTO.question = question;
        tableRowDTO.answerColumn = answerColumn;

        return tableRowDTO;
    }

    public static TableRowDTO create(String tableId, Set<TableCell> rowEntries, Integer row,
                                     String question, Integer answerColumn, String tableExplanation) {
        TableRowDTO tableRowDTO = new TableRowDTO();
        tableRowDTO.tableId = tableId;
        tableRowDTO.rowEntries = rowEntries.stream().map(
                rowEntry -> new TableCellDTO(
                        rowEntry.getRowName(), rowEntry.getColumnId(), rowEntry.getCellValue()
                ))
                .collect(Collectors.toSet());
        tableRowDTO.row = row;
        tableRowDTO.question = question;
        tableRowDTO.answerColumn = answerColumn;
        tableRowDTO.content = tableExplanation;
        return tableRowDTO;
    }

    public String getTableId() {
        return tableId;
    }

    public Set<TableCellDTO> getRowEntries() {
        return rowEntries;
    }

    public String getQuestion() {
        return question;
    }

    public Integer getRow() {
        return row;
    }

    public Integer getAnswerColumn() {
        return answerColumn;
    }

    public String getContent() {
        return content;
    }

    public void setRowEntries() {
        rowEntries = new HashSet<>();
    }
}
