package org.name.microservices;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

@Path("")
@RegisterRestClient
public interface GenesisApiClient {

    @Path("/catalogue/tables")
    @GET
    public TablesAnswer getTables(@QueryParam(value = "username") String username,
                                  @QueryParam(value = "password") String password,
                                  @QueryParam(value = "pagelength") Integer pageLength);


    @Path("/data/table")
    @GET
    public GenesisTable getTable(@QueryParam(value = "username") String username,
                          @QueryParam(value = "password") String password,
                          @QueryParam(value = "name") String name);



}
