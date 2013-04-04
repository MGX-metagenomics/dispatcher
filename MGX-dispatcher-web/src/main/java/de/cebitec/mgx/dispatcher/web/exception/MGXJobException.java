
package de.cebitec.mgx.dispatcher.web.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 *
 * @author sjaenick
 */
@ApplicationException
public class MGXJobException extends WebApplicationException {
    
    public MGXJobException(Throwable cause) {
        super(cause);
    }

    public MGXJobException(String message) {
        super(Response.status(Status.BAD_REQUEST).entity(message).type(MediaType.TEXT_PLAIN).build());
    }
}
