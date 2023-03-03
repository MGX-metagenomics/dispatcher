package de.cebitec.mgx.dispatcher.web.exception;

import jakarta.ejb.ApplicationException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.io.Serial;

/**
 *
 * @author sjaenick
 */
@ApplicationException
public class MGXJobException extends WebApplicationException {

    @Serial
    private static final long serialVersionUID = 1L;

    public MGXJobException(Throwable cause) {
        super(cause);
    }

    public MGXJobException(String message) {
        super(Response.status(Status.BAD_REQUEST).entity(message).type(MediaType.TEXT_PLAIN).build());
    }
}
