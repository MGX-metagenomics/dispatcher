
package de.cebitec.mgx.dispatcher.web.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

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
