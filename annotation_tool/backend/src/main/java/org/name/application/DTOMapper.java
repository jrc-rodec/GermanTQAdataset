package org.name.application;

import org.name.controller.TableCellDTO;
import org.name.controller.TableRowDTO;
import org.name.domain.TableCell;
import org.name.domain.TableRow;

import java.util.stream.Collectors;

public class DTOMapper {
    public static TableRowDTO mapTableRowToRowDTO(TableRow row){
        TableRowDTO rowDTO = TableRowDTO.create(
                row.tableId(), row.rowEntries(), row.row(), row.question(), row.answerColumn(), row.content()
        );
        rowDTO.setRowEntries();
        row.rowEntries().forEach(
                rowEntry -> rowDTO.getRowEntries().add(new TableCellDTO(rowEntry.getRowName(), rowEntry.getColumnId(), rowEntry.getCellValue()))
        );
        return rowDTO;
    }

    public static TableRow mapTableRowDTOtoTableRow(TableRowDTO tableRowDTO) {
        return TableRow.create(tableRowDTO.getTableId(),
                tableRowDTO.getRowEntries().stream().map(rowEntry -> new TableCell(
                        rowEntry.getRowName(), rowEntry.getColumnId(), rowEntry.getCellValue())
                        ).collect(Collectors.toSet()),
                tableRowDTO.getQuestion(), tableRowDTO.getRow(), tableRowDTO.getAnswerColumn(), true);
    }
}
