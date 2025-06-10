package org.name.api;

import org.name.application.interfaces.TableRowService;
import org.name.controller.TableRowDTO;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/tables/rows")
public class CSVTableRowAPI {

    @Inject
    TableRowService tableRowService;

    @Path("/get5UnansweredRows")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TableRowDTO> get5UnansweredRows() {
        return tableRowService.get5RandomRows();
    }

    @Path("/submit5UnansweredRows")
    @POST
    public void submit5UnansweredRows(List<TableRowDTO> answeredTableRowDTOs){
        tableRowService.updateRows(answeredTableRowDTOs);
    }

    @Path("/setAsAnswered")
    @POST
    public void setAsAnswered(List<TableRowDTO> answeredTableRowDTOs){
        tableRowService.updateRows(answeredTableRowDTOs);
    }
}
