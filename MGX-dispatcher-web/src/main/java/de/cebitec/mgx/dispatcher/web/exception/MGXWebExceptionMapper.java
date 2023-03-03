
package de.cebitec.mgx.dispatcher.web.exception;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;


/**
 *
 * @author sjaenick
 */
@Provider
public class MGXWebExceptionMapper implements ExceptionMapper<MGXWebException> {

    @Override
    public Response toResponse(MGXWebException ex) {
        return Response.status(ex.status()).
                entity(ex.getMessage()).
                type(MediaType.TEXT_PLAIN).
                build();
    }
}
