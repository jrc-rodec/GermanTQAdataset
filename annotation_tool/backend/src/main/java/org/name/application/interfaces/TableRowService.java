package org.name.application.interfaces;

import org.name.controller.TableRowDTO;

import java.util.List;

public interface TableRowService {

    List<TableRowDTO> get5RandomRows();

    boolean updateRows(List<TableRowDTO> updatedTableRows);

}
