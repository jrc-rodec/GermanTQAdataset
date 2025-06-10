package org.name.domain.repositories;

import org.name.domain.TableRow;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface TableRepository {
    Set<TableRow> getAllTableRows();

    Set<TableRow> getUnansweredTableRows();

    void updateTableRow(TableRow updatedTableRow);

    Optional<TableRow> getTableRowByTableIdAndRowNumber(String tableId, Integer row);


    void insertTable(List<TableRow> table);
}
