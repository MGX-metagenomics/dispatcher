
package de.cebitec.mgx.dispatcher.web.exception;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;


/**
 *
 * @author sjaenick
 */
@Provider
public class MGXJobExceptionMapper implements ExceptionMapper<MGXJobException> {

    @Override
    public Response toResponse(MGXJobException ex) {
        return Response.status(Status.BAD_REQUEST).
                entity(ex.getMessage()).
                type(MediaType.TEXT_PLAIN).
                build();
    }
}
