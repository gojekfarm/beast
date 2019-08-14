package com.gojek.beast.protomapping;

import com.gojek.beast.models.ExternalCallException;
import com.gojek.beast.models.UpdateBQTableRequest;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

@Slf4j
public class UpdateTableService {
    public String updateBigQuerySchema(String updateTableURL, UpdateBQTableRequest request) throws ExternalCallException {
        ClientConfig config = new ClientConfig();
        config.register(JacksonJsonProvider.class);
        Client client = ClientBuilder.newClient(config);
        Response updateTableResponse = client.target(updateTableURL).request().post(Entity.json(request), Response.class);
        if (updateTableResponse.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new ExternalCallException("Get ProtoMapping URL returned: " + updateTableResponse.getStatus() + "error: " + updateTableResponse.readEntity(String.class));
        } else {
            log.info("Successfully updated table on schema update");
            String response = updateTableResponse.readEntity(String.class);
            return response;
        }
    }

    public String getProtoMappingFromRemoteURL(String getProtoMappingURL, String proto) throws ExternalCallException {
        Client client = ClientBuilder.newClient();
        String protoMappingURL = getProtoMappingURL + "/" + proto;
        Response protoMappingResponse = client.target(UriBuilder.fromUri(protoMappingURL)).request().get(Response.class);
        if (protoMappingResponse.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new ExternalCallException("Get ProtoMapping URL returned: " + protoMappingResponse.getStatus() + "error: " + protoMappingResponse.readEntity(String.class));
        }
        String protoMapping = protoMappingResponse.readEntity(String.class);
        return protoMapping;
    }
}
