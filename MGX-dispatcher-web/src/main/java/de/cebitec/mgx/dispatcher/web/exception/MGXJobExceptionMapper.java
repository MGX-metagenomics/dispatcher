
package de.cebitec.mgx.dispatcher.web.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

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
