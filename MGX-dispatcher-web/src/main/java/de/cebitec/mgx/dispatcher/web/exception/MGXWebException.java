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
public class MGXWebException extends WebApplicationException {

    @Serial
    private static final long serialVersionUID = 1L;

    private Status http_status = null;

//    public MGXWebException(Throwable cause) {
//        super(cause);
//    }
    public MGXWebException(String message) {
        super(Response.status(Status.BAD_REQUEST).entity(message).type(MediaType.TEXT_PLAIN).build());
        assert message != null;
        assert !"".equals(message);
    }

    public MGXWebException(Status status, String message) {
        super(Response.status(status).entity(message).type(MediaType.TEXT_PLAIN).build());
        assert message != null;
        assert !"".equals(message);
        http_status = status;
    }

    public Status status() {
        if (http_status != null) {
            return http_status;
        } else {
            return Status.INTERNAL_SERVER_ERROR;
        }
    }
}
